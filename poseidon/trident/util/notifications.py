import os
import requests
import logging
import json

from trident.util import general


conf = general.config


def afsys_send_email(to,
                     subject,
                     html_content,
                     files=None,
                     dryrun=False,
                     cc=None,
                     bcc=None,
                     mime_subtype='mixed',
                     **kwargs):
    """
    Override airflow internal mail system. Notify via email.
    :param to: comma separated string of email addresses
    :type to: string
    :param subject: email subject
    :type subject: string
    :param html_content: email html content
    :type html_content: string
    :type cc: string
    :type bcc: string
    :type mime_subtype: string

    """
    if conf['mail_notify'] == 1:
        logging.info("Dispatching email: " + subject)
        template_data = {
            'html_content': html_content,
            'preheader': subject
        }
        # Dispatch SWU email
        send_email_swu(
            to=to,
            template_data=template_data,
            template_id=conf['mail_swu_sys_tpl'],
            dispatch_type='airflow_alert',
            subject='Airflow Alert')






# https://www.sendwithus.com/docs/api#sending-emails
def send_email_swu(to,
                   template_id,
                   dispatch_type,
                   subject,
                   template_data=None,
                   dispatch_meta=None,
                   cc=None,
                   bcc=None,
                   headers=None,
                   locale='en-US',
                   version_name=None):

    """
    :param to: comma separated string of email addresses
    :type to: string
    :param subject: email subject
    :type subject: string
    :param html_content: email html content
    :type html_content: string
    :type cc: string
    :type bcc: string
    :type mime_subtype: string
    """
    request_url = 'https://api.sendwithus.com/api/v1/send'
    api_key = conf['mail_swu_key']


    to = get_email_address_list(to)
    cc = get_email_address_list(cc)
    bcc = get_email_address_list(bcc)

    template_data = template_data or {}
    dispatch_meta = dispatch_meta or {}


    default_receivers = get_email_address_list(conf['mail_default_receivers'])

    all_receivers = []

    # Add default receivers to to list
    to.extend(default_receivers)

    # Dedup the to list
    to = list(set(to))

    all_receivers = all_receivers + to

    # If we have > 1 address, move extras to cc;
    if len(to) > 1:
        if cc == None:
            cc = []
        while len(to) > 1:
            cc.append(to.pop())

    # Add subject to template data:
    template_data['subject'] = subject

    payload = {
        'template': template_id,
        'template_data': template_data,
        'locale': locale,
        'recipient': {
            'address': to.pop()
        },
        'sender': {
            'name': conf['mail_from_name'],
            'address': conf['mail_from_addr'],
            'reply_to': conf['mail_from_reply_to']
        }
    }


    for elisttype, elist in {'cc': cc, 'bcc': bcc}.items():
        if elist is not None and len(elist) > 0:
            all_receivers + elist
            payload[elisttype] = []

            for address in elist:
                payload[elisttype].append({"address": address})



    # Dedupe all_receivers list
    all_receivers = list(set(all_receivers))

    r = requests.post(
        request_url, auth=(api_key, ''), data=json.dumps(payload))
    r.raise_for_status()
    logging.info('Dispatched email with SWU: ' + subject)

    ## Notify Keen about e-mail dispatch
    for receiver in all_receivers:
        keen_payload = {
          'receiver': receiver,
          'template_id': template_id,
          'template_data': template_data,
          'dispatch_type': dispatch_type,
          'dispatch_meta': dispatch_meta,
          'subject': subject,
          'locale': locale,
          'version_name': version_name
        }
        logging.info('Notify keen about email to: ' + receiver)
        notify_keen(keen_payload,
                    "poseidon_{}_dispatched_emails".format(conf['env'].lower()))



def notify(context):
    """Dispatch payload notification."""
    # Check local and test mode
    task_instance = context['task_instance']
    payload = {
        "run_date": context['execution_date'].isoformat(),
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "test_mode": task_instance.test_mode,
        "try_number": task_instance.try_number,
        "duration": task_instance.duration,
        "state": task_instance.state,
        "operator": task_instance.operator,
        "job_id": task_instance.job_id
    }
    notify_keen(payload, conf['keen_ti_collection'])


def notify_keen(payload, collection, raise_for_status = False):
    """ TODO - move this to keen operator """
    if conf['keen_notify'] == 1:
        url = 'https://api.keen.io/3.0/projects/{}/events/{}'.format(
            conf['keen_project_id'], collection)

        headers = {
            'Authorization': conf['keen_write_key'],
            'Content-Type': 'application/json'
        }

        request = requests.post(url, headers=headers, json=payload)

        # Raise for status if requested
        if (raise_for_status is True):
            request.raise_for_status()

        logging.info("Dispatched keen notification to {} collection".format(collection))
    else:
        logging.info("Keen notifications to {} collection disabled".format(collection))


def get_email_address_list(address_string):
    if isinstance(address_string, str):
        if ',' in address_string:
            address_string = address_string.split(',')
        elif ';' in address_string:
            address_string = address_string.split(';')
        else:
            address_string = [address_string]

    return address_string
