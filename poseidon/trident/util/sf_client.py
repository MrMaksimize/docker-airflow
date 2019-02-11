"""Utilities for SF Login."""
import requests
import xml.dom.minidom
import pandas as pd
import csv
import json
import logging
from trident.util import general


conf = general.config


def getUniqueElementValueFromXmlString(xmlString, elementName):
    """Extract an element value from an XML string.

    For example, invoking
    getUniqueElementValueFromXmlString(
        '<?xml version="1.0" encoding="UTF-8"?><foo>bar</foo>', 'foo')
    should return the value 'bar'.
    """
    xmlStringAsDom = xml.dom.minidom.parseString(xmlString)
    elementsByName = xmlStringAsDom.getElementsByTagName(elementName)
    elementValue = None
    if len(elementsByName) > 0:
        elementValue = elementsByName[0].toxml().replace(
            '<' + elementName + '>', '').replace('</' + elementName + '>', '')
    return elementValue


class Salesforce(object):
    """SF Client Class."""
    def __init__(self,
                 username=None,
                 password=None,
                 security_token=None,
                 client_id='DataSD_Poseidon',
                 sf_version='39.0',
                 domain='sdgov.my'):

        self.sf_version = sf_version
        self.domain = domain
        self.client_id = client_id
        self.session_id = None

        # Security Token Soap request body
        login_soap_request_body = """<?xml version="1.0" encoding="utf-8" ?>
        <env:Envelope
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:env="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:urn="urn:partner.soap.sforce.com">
            <env:Header>
                <urn:CallOptions>
                    <urn:client>{client_id}</urn:client>
                    <urn:defaultNamespace>sf</urn:defaultNamespace>
                </urn:CallOptions>
            </env:Header>
            <env:Body>
                <n1:login xmlns:n1="urn:partner.soap.sforce.com">
                    <n1:username>{username}</n1:username>
                    <n1:password>{password}{token}</n1:password>
                </n1:login>
            </env:Body>
        </env:Envelope>""".format(
            username=username,
            password=password,
            token=security_token,
            client_id=client_id)

        soap_url = 'https://{domain}.salesforce.com/services/Soap/u/{sf_version}'

        soap_url = soap_url.format(domain=domain, sf_version=sf_version)

        login_soap_request_headers = {
            'content-type': 'text/xml',
            'charset': 'UTF-8',
            'SOAPAction': 'login'
        }
        response = requests.post(
            soap_url,
            login_soap_request_body,
            headers=login_soap_request_headers)

        if response.status_code != 200:
            raise Exception("Login Failed")

        self.session_id = getUniqueElementValueFromXmlString(response.content,
                                                             'sessionId')
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.session_id,
            'X-PrettyPrint': '1'
        }

    def get_report_df(self, report_id):
        """Get SF report and return dataframe."""
        url = "https://{domain}.salesforce.com/{report_id}?view=d&snip&export=1&enc=UTF-8&xf=csv"
        url = url.format(report_id=report_id, domain=self.domain)
        try:
            resp = requests.get(url,
                                headers=self.headers,
                                cookies={'sid': self.session_id})

            lines = resp.content.splitlines()
            reader = csv.reader(lines)
            data = list(reader)
            data = data[:-7]

            df = pd.DataFrame(data)
            df.columns = df.iloc[0]
            df = df.drop(0)

            return df

        except Exception, e:
            logging.error(e)




    def get_report_csv(self, report_id):
        # TODO - this needs abstraction for file name
        """Get SF report and return csv file."""
        temp_gid = conf['temp_data_dir'] + '/gid_temp.csv'
        url = "https://{domain}.salesforce.com/{report_id}?view=d&snip&export=1&enc=ISO-8859-1&xf=csv"
        url = url.format(report_id=report_id, domain=self.domain)
        try:
            resp = requests.get(url,
                                headers=self.headers,
                                cookies={'sid': self.session_id})

            lines = resp.content.splitlines()
            reader = csv.reader(lines)
            data = list(reader)
            data = data[:-7]

            with open(temp_gid, 'wb') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerows(data)

            return "Retrieved last GID data"

        except Exception, e:
            logging.error(e)

    def get_query_records(self, query_string):
        """Query SF and return JSON response."""
        url = "https://{domain}.salesforce.com/services/data/v{sf_version}/query/?q={query_string}"
        url = url.format(domain=self.domain,
                         sf_version=self.sf_version,
                         query_string=query_string)

        try:
            resp = requests.get(url,
                                headers=self.headers,
                                cookies={'sid': self.session_id})
            return resp.json()

        except Exception, e:
            logging.error(e)

    def get_query_more(self, next_page_url):
        """Query extra page and return JSON response."""
        url = "https://{domain}.salesforce.com{next_page_url}"
        url = url.format(domain=self.domain,
                         next_page_url=next_page_url)
        try:
            resp = requests.get(url,
                                headers=self.headers,
                                cookies={'sid': self.session_id})

            return resp.json()

        except Exception, e:
            logging.error(e)

    def get_query_all(self, query_string):
        """Combine 'query' and 'query more' methods to 'query all'."""
        result = self.get_query_records(query_string)
        all_records = []

        while True:
            all_records.extend(result['records'])

            if not result['done']:
                result = self.get_query_more(result['nextRecordsUrl'])
            else:
                break

        result['records'] = all_records

        return result
