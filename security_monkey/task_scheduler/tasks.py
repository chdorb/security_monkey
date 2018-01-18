"""
.. module: security_monkey.task_scheduler.tasks
    :platform: Unix
    :synopsis: Sets up the Celery task scheduling for watching, auditing, and reporting changes in the environment.

.. version:: $$VERSION$$
.. moduleauthor:: Mike Grima <mgrima@netflix.com>

"""
import time
import traceback

from celery.schedules import crontab

from security_monkey import app, db, jirasync, sentry
from security_monkey.alerter import Alerter
from security_monkey.common.utils import find_modules
from security_monkey.datastore import Account, store_exception, clear_old_exceptions
from security_monkey.monitors import get_monitors, get_monitors_and_dependencies
from security_monkey.reporter import Reporter
from security_monkey.task_scheduler.util import CELERY
from sqlalchemy.exc import OperationalError, InvalidRequestError, StatementError


def setup():
    """Load the required data for scheduling tasks"""
    find_modules('alerters')
    find_modules('watchers')
    find_modules('auditors')


def purge_it():
    """Purge the existing Celery queue"""
    app.logger.debug("Purging the Celery tasks awaiting to execute")
    CELERY.control.purge()
    app.logger.debug("Completed the Celery purge.")


@CELERY.on_after_configure.connect
def setup_the_tasks(sender, **kwargs):
    setup()

    # Purge out all current tasks waiting to execute:
    purge_it()

    # Add all the tasks:
    try:
        # TODO: Investigate options to have the scheduler skip different types of accounts
        accounts = Account.query.filter(Account.third_party == False).filter(Account.active == True).all()  # noqa
        for account in accounts:
            app.logger.info("[ ] Scheduling tasks for {type} account: {name}".format(type=account.type.name,
                                                                                     name=account.name))
            rep = Reporter(account=account.name)
            for monitor in rep.all_monitors:
                if monitor.watcher:
                    app.logger.debug("[{}] Scheduling for technology: {}".format(account.type.name,
                                                                                 monitor.watcher.index))
                    interval = monitor.watcher.get_interval() * 60

                    # Start the task immediately:
                    task_account_tech.apply_async((account.name, monitor.watcher.index))
                    app.logger.debug("[-->] Scheduled immediate task")

                    # Schedule it based on the schedule:
                    sender.add_periodic_task(interval, task_account_tech.s(account.name, monitor.watcher.index))
                    app.logger.debug("[+] Scheduled task to occur every {} minutes".format(interval))

                    # Also schedule a manual audit changer just in case it doesn't properly
                    # audit (only for non-batched):
                    if not monitor.batch_support:
                        sender.add_periodic_task(
                            crontab(hour=10, day_of_week="mon-fri"), task_audit.s(account.name, monitor.watcher.index))
                        app.logger.debug("\t\t[+] Scheduled task for tech: {} for audit")

                    app.logger.debug("[{}] Completed scheduling for technology: {}".format(account.name,
                                                                                           monitor.watcher.index))
            app.logger.debug("[+] Completed scheduling tasks for account: {}".format(account.name))

        # Schedule the task for clearing out old exceptions:
        app.logger.info("Scheduling task to clear out old exceptions.")
        sender.add_periodic_task(crontab(hour=3, minute=0), clear_expired_exceptions.s())

    except Exception as e:
        if sentry:
            sentry.captureException()
        app.logger.error("[X] Scheduler Exception: {}".format(e))
        app.logger.error(traceback.format_exc())
        store_exception("scheduler", None, e)


@CELERY.task(bind=True, max_retries=3)
def task_account_tech(self, account_name, technology_name):
    setup()
    app.logger.info("[ ] Executing Celery task for account: {}, technology: {}".format(account_name, technology_name))
    time1 = time.time()

    try:
        reporter_logic(account_name, technology_name)

        time2 = time.time()
        app.logger.info('[@] Run Account for Technology (%s/%s) took %0.1f s' % (account_name,
                                                                                 technology_name, (time2 - time1)))
        app.logger.info(
            "[+] Completed Celery task for account: {}, technology: {}".format(account_name, technology_name))
    except Exception as e:
        if sentry:
            sentry.captureException()
        app.logger.error("[X] Task Account Scheduler Exception ({}/{}): {}".format(account_name, technology_name, e))
        app.logger.error(traceback.format_exc())
        store_exception("scheduler-exception-on-watch", None, e)
        raise self.retry(exc=e)


@CELERY.task(bind=True, max_retries=3)
def task_audit(self, account_name, technology_name):
    setup()

    app.logger.info("[ ] Executing Celery task to audit changes for Account: {} Technology: {}".format(account_name,
                                                                                                       technology_name))
    try:
        audit_changes([account_name], [technology_name], True)

        app.logger.info("[+] Completed Celery task for account: {}, technology: {}".format(account_name,
                                                                                           technology_name))

    except Exception as e:
        if sentry:
            sentry.captureException()
        app.logger.error("[X] Task Audit Scheduler Exception ({}/{}): {}".format(account_name, technology_name, e))
        app.logger.error(traceback.format_exc())
        store_exception("scheduler-exception-on-audit", None, e)
        self.retry(exc=e)


@CELERY.task()
def clear_expired_exceptions():
    app.logger.info("[ ] Clearing out exceptions that have an expired TTL...")
    clear_old_exceptions()
    app.logger.info("[-] Completed clearing out exceptions that have an expired TTL.")


def reporter_logic(account_name, technology_name):
    """Logic for the run change reporter"""
    try:
        # Watch and Audit:
        monitors = find_changes(account_name, technology_name)

        # Alert:
        app.logger.info("[ ] Sending alerts (if applicable) for account: {}, technology: {}".format(account_name,
                                                                                                    technology_name))
        Alerter(monitors, account=account_name).report()
    except (OperationalError, InvalidRequestError, StatementError) as e:
        app.logger.exception("[X] Database error processing account %s - technology %s cleaning up session.",
                             account_name, technology_name)
        db.session.remove()
        store_exception("scheduler-task-account-tech", None, e)
        raise e


def manual_run_change_reporter(accounts):
    """Manual change reporting from the command line"""
    app.logger.info("[ ] Executing manual change reporter task...")

    try:
        for account in accounts:
            time1 = time.time()
            rep = Reporter(account=account)

            for monitor in rep.all_monitors:
                if monitor.watcher:
                    app.logger.info("[ ] Running change finder for "
                                    "account: {} technology: {}".format(account, monitor.watcher.index))
                    reporter_logic(account, monitor.watcher.index)

            time2 = time.time()
            app.logger.info('[@] Run Account %s took %0.1f s' % (account, (time2 - time1)))

        app.logger.info("[+] Completed manual change reporting.")
    except (OperationalError, InvalidRequestError, StatementError) as e:
        app.logger.exception("[X] Database error processing cleaning up session.")
        db.session.remove()
        store_exception("scheduler-run-change-reporter", None, e)
        raise e


def manual_run_change_finder(accounts, technologies):
    """Manual change finder"""
    app.logger.info("[ ] Executing manual find changes task...")

    try:
        for account in accounts:
            time1 = time.time()

            for tech in technologies:
                find_changes(account, tech)

            time2 = time.time()
            app.logger.info('[@] Run Account %s took %0.1f s' % (account, (time2 - time1)))
        app.logger.info("[+] Completed manual change finder.")
    except (OperationalError, InvalidRequestError, StatementError) as e:
        app.logger.exception("[X] Database error processing cleaning up session.")
        db.session.remove()
        store_exception("scheduler-run-change-reporter", None, e)
        raise e


def find_changes(account_name, monitor_name, debug=True):
    """
        Runs the watcher and stores the result, re-audits all types to account
        for downstream dependencies.
    """
    monitors = get_monitors(account_name, [monitor_name], debug)
    for mon in monitors:
        cw = mon.watcher
        app.logger.info("[-->] Looking for changes in account: {}, technology: {}".format(account_name, cw.index))
        if mon.batch_support:
            batch_logic(mon, cw, account_name, debug)
        else:
            # Just fetch normally...
            (items, exception_map) = cw.slurp()
            cw.find_changes(current=items, exception_map=exception_map)
            cw.save()

    # Batched monitors have already been monitored, and they will be skipped over.
    audit_changes([account_name], [monitor_name], False, debug)
    db.session.close()

    return monitors


def audit_changes(accounts, monitor_names, send_report, debug=True, skip_batch=True):
    """
    Audits changes in the accounts
    :param accounts:
    :param monitor_names:
    :param send_report:
    :param debug:
    :param skip_batch:
    :return:
    """
    for account in accounts:
        monitors = get_monitors_and_dependencies(account, monitor_names, debug)
        for monitor in monitors:
            # Skip batch support monitors... They have already been monitored.
            if monitor.batch_support and skip_batch:
                continue

            app.logger.debug("[-->] Auditing account: {}, technology: {}".format(account, monitor.watcher.index))
            _audit_changes(account, monitor.auditors, send_report, debug)


def batch_logic(monitor, current_watcher, account_name, debug):
    """
    Performs the batch watcher finding and auditing.

    TODO: Investigate how this could, in the future, be set to parallelize the batches.
    :param monitor:
    :param current_watcher:
    :param account_name:
    :param debug:
    :return:
    """
    # Fetch the full list of items that we need to obtain:
    _, exception_map = current_watcher.slurp_list()
    if len(exception_map) > 0:
        # Get the location tuple to collect the region:
        location = exception_map.keys()[0]
        if len(location) > 2:
            region = location[2]
        else:
            region = "unknown"

        app.logger.error("[X] Exceptions have caused nothing to be fetched for {technology}"
                         "/{account}/{region}..."
                         " CANNOT CONTINUE FOR THIS WATCHER!".format(technology=current_watcher.i_am_plural,
                                                                     account=account_name,
                                                                     region=region))
        return

    while not current_watcher.done_slurping:
        app.logger.debug("[-->] Fetching a batch of {batch} items for {technology}/{account}.".format(
            batch=current_watcher.batched_size, technology=current_watcher.i_am_plural, account=account_name
        ))
        (items, exception_map) = current_watcher.slurp()

        audit_items = current_watcher.find_changes(current=items, exception_map=exception_map)
        _audit_specific_changes(monitor, audit_items, False, debug)

    # Delete the items that no longer exist:
    app.logger.debug("[-->] Deleting all items for {technology}/{account} that no longer exist.".format(
        technology=current_watcher.i_am_plural, account=account_name
    ))
    current_watcher.find_deleted_batch(account_name)


def _audit_changes(account, auditors, send_report, debug=True):
    """ Runs auditors on all items """
    try:
        for au in auditors:
            au.items = au.read_previous_items()
            au.audit_objects()
            au.save_issues()
            if send_report:
                report = au.create_report()
                au.email_report(report)

            if jirasync:
                app.logger.info('[-->] Syncing {} issues on {} with Jira'.format(au.index, account))
                jirasync.sync_issues([account], au.index)
    except (OperationalError, InvalidRequestError, StatementError) as e:
        app.logger.exception("[X] Database error processing accounts %s, cleaning up session.", account)
        db.session.remove()
        store_exception("scheduler-audit-changes", None, e)


def _audit_specific_changes(monitor, audit_items, send_report, debug=True):
    """
    Runs the auditor on specific items that are passed in.
    :param monitor:
    :param audit_items:
    :param send_report:
    :param debug:
    :return:
    """
    try:
        for au in monitor.auditors:
            au.items = audit_items
            au.audit_objects()
            au.save_issues()
            if send_report:
                report = au.create_report()
                au.email_report(report)

            if jirasync:
                app.logger.info('[-->] Syncing {} issues on {} with Jira'.format(au.index, monitor.watcher.accounts[0]))
                jirasync.sync_issues(monitor.watcher.accounts, au.index)
    except (OperationalError, InvalidRequestError, StatementError) as e:
        app.logger.exception("[X] Database error processing accounts %s, cleaning up session.",
                             monitor.watcher.accounts[0])
        db.session.remove()
        store_exception("scheduler-audit-changes", None, e)
