import csv
from time import sleep

import dateutil
import mws
from aiohttp import web

from lovat import settings
from lovat.auth import user_login
from lovat.services.utils import SERVICE_ID_OTHER, SERVICE_ID_GOODS, REVERSE_CHARGE_ID_GOODS
from io import StringIO

from lovat.transactions.enums import TransactionType
from lovat.websites.enums import WebsiteStatus, WebsitePlatform


def get_list_transaction(min_date, max_date, extra_data={}):
    r = mws_reports(extra_data)
    report_request_id = get_report_request_id(r, min_date, max_date)
    if not report_request_id:
        return None, 'not report_request_id'
    status = get_report_status(r, report_request_id)
    if status in ('_CANCELLED_', '_DONE_NO_DATA_'):
        return None, status
    report_id = get_report_id(r, report_request_id)
    if not report_id:
        return None, '_DONE_NO_DATA_'
    csvfile = get_report(r, report_id)
    csvfile = StringIO(csvfile.decode('utf-8'))
    dialect = csv.Sniffer().sniff(csvfile.readline(), delimiters="\t,;")
    csvfile.seek(0)
    csvreader = csv.DictReader(csvfile, dialect=dialect)
    return csv_2_transaction(csvreader), status


def mws_reports(extra_data={}, count=0):
    count += 1
    if count == 100:
        return
    try:
        report = mws.Reports(access_key=settings.amazon_access_key, secret_key=settings.amazon_secret_key,
                             account_id=extra_data['seller_id'], auth_token=extra_data['auth_token'], region='UK')
        sleep(10)
    except mws.MWSError as e:
        if not ('RequestThrottled' in e.args[0]):
            return
        sleep(10)
        report = mws_reports(extra_data, count)
    return report


def get_report_request_id(r, min_date, max_date, count=0):
    count += 1
    if count == 20:
        return
    try:
        report = r.request_report(report_type='_GET_VAT_TRANSACTION_DATA_', start_date=min_date, end_date=max_date).parsed
        return report['ReportRequestInfo']['ReportRequestId'].value
    except mws.MWSError as e:
        sleep(10)
        return get_report_request_id(r, min_date, max_date, count)


def get_report_id(r, report_request_id, count=0):
    count += 1
    if count == 100:
        return
    try:
        report = r.get_report_list(requestids=report_request_id).parsed
        if not report.get('ReportInfo'):
            sleep(10)
            return get_report_id(r, report_request_id, count)
        return report['ReportInfo']['ReportId'].value
    except mws.MWSError as e:
        # if not ('RequestThrottled' in e.args[0]):
        #     return
        sleep(10)
        return get_report_id(r, report_request_id, count)


def get_report_status(r, report_request_id, count=0):
    count += 1
    if count == 100:
        return
    try:
        report = r.get_report_request_list(requestids=report_request_id).parsed
        if report.get('ReportRequestInfo'):
            status = report['ReportRequestInfo']['ReportProcessingStatus'].value
            if status == '_IN_PROGRESS_':
                return get_report_status(r, report_request_id, count)
            else:
                return status
        return report['ReportInfo']['ReportId'].value
    except mws.MWSError as e:
        # if not ('RequestThrottled' in e.args[0]):
        #     return
        sleep(10)
        return get_report_status(r, report_request_id, count)


def get_report(r, report_id, count=0):
    count += 1
    if count == 100:
        return
    try:
        report = r.get_report(report_id=report_id).parsed
    except mws.MWSError as e:
        # if not ('RequestThrottled' in e.args[0]):
        #     return
        sleep(10)
        report = get_report(r, report_id, count)
    return report


# def pars_zip(t):
#     if t['delivery_zip'] and t['delivery_address'] == 'GB':
#         zip2 = t['delivery_zip'][0:2].upper()
#         if zip2 == 'BT':
#             t['departure_state'] = 'GB-NIR'
#         elif zip2 == 'IM':
#             t['delivery_address'] = 'IM'
#         elif zip2 == 'JE':
#             t['delivery_address'] = 'JE'
#         elif zip2 == 'GY':
#             t['delivery_address'] = 'GG'
#
#     if t['arrival_zip'] and t['arrival_country'] == 'GB':
#         zip2 = t['arrival_zip'][0:2].upper()
#         if zip2 == 'BT':
#             t['arrival_state'] = 'GB-NIR'
#         elif zip2 == 'IM':
#             t['arrival_country'] = 'IM'
#         elif zip2 == 'JE':
#             t['arrival_country'] = 'JE'
#         elif zip2 == 'GY':
#             t['arrival_country'] = 'GG'


def csv_2_transaction(csvreader):
    list_transaction = list()
    for transaction in csvreader:
        order_status = transaction['TRANSACTION_TYPE']
        if order_status in ['SALE', 'REFUND', 'COMMINGLING_SELL', 'COMMINGLING_BUY']:
            t = dict()
            is_refund_transaction = order_status == 'REFUND'
            if order_status == 'COMMINGLING_BUY':
                t['transaction_type'] = TransactionType.incoming_VAT_invoice.value
            t['transaction_status'] = 'refund' if is_refund_transaction else 'success'
            t['transaction_id'] = '{}-{}-{}-{}'.format(transaction['TRANSACTION_EVENT_ID'], transaction['ASIN'], transaction['ACTIVITY_TRANSACTION_ID'], t['transaction_status'])
            t['parent_transaction_id'] = '{}-{}'.format(transaction['TRANSACTION_EVENT_ID'], transaction['ASIN'])
            t['transaction_datetime'] = str(dateutil.parser.parse(transaction['TRANSACTION_COMPLETE_DATE'], dayfirst=True))
            if not transaction.get('TOTAL_ACTIVITY_VALUE_AMT_VAT_INCL'):
                continue
            t['transaction_sum'] = float(transaction['TOTAL_ACTIVITY_VALUE_AMT_VAT_INCL'])
            t['currency'] = transaction['TRANSACTION_CURRENCY_CODE']
            t['transaction_status'] = 'refund' if is_refund_transaction else 'success'
            t['delivery_address'] = transaction['SALE_ARRIVAL_COUNTRY']
            t['delivery_city'] = transaction['DEPATURE_CITY']
            t['delivery_zip'] = transaction['DEPARTURE_POST_CODE']
            t['arrival_country'] = transaction['SALE_ARRIVAL_COUNTRY']
            t['arrival_city'] = transaction['ARRIVAL_CITY']
            t['arrival_zip'] = transaction['ARRIVAL_POST_CODE']
            # pars_zip(t)
            t['vat_inv_number'] = transaction['VAT_INV_NUMBER']
            t['departure_address'] = transaction['SALE_DEPART_COUNTRY']
            t['country_of_report'] = transaction['TRANSACTION_SELLER_VAT_NUMBER_COUNTRY']  # TRANSACTION_SELLER_VAT_NUMBER_COUNTRY (BW)
            t['country_of_buyer'] = transaction['BUYER_VAT_NUMBER_COUNTRY']  # BUYER_VAT_NUMBER_COUNTRY (BY)
            t['vat_number_of_buyer'] = transaction['BUYER_VAT_NUMBER']  # BUYER_VAT_NUMBER (BZ)
            t['seller_vat_number'] = transaction['TRANSACTION_SELLER_VAT_NUMBER']
            t['buyer_name'] = transaction['BUYER_NAME']
            t['seller_name'] = transaction['SUPPLIER_NAME']

            t['merchant_id'] = transaction['UNIQUE_ACCOUNT_IDENTIFIER']
            data_json = {}
            data_json['activity_transaction_id'] = transaction['ACTIVITY_TRANSACTION_ID']
            data_json['tax_calculation_date'] = ''
            if transaction.get('TAX_CALCULATION_DATE'):
                data_json['tax_calculation_date'] = dateutil.parser.parse(transaction['TAX_CALCULATION_DATE'], dayfirst=True).strftime("%Y-%m-%d %H:%M:%S")
                data_json['date_payment_received'] = data_json['tax_calculation_date']
            data_json['item_description'] = transaction['ITEM_DESCRIPTION']
            data_json['price_of_items_amt_vat_excl'] = transaction['PRICE_OF_ITEMS_AMT_VAT_EXCL']
            data_json['promo_price_of_items_amt_vat_excl'] = transaction['PROMO_PRICE_OF_ITEMS_AMT_VAT_EXCL']
            data_json['total_price_of_items_amt_vat_excl'] = transaction['TOTAL_PRICE_OF_ITEMS_AMT_VAT_EXCL']
            data_json['ship_charge_amt_vat_excl'] = transaction['SHIP_CHARGE_AMT_VAT_EXCL']
            data_json['promo_ship_charge_amt_vat_excl'] = transaction['PROMO_SHIP_CHARGE_AMT_VAT_EXCL']
            data_json['total_ship_charge_amt_vat_excl'] = transaction['TOTAL_SHIP_CHARGE_AMT_VAT_EXCL']
            data_json['total_activity_value_amt_vat_excl'] = transaction['TOTAL_ACTIVITY_VALUE_AMT_VAT_EXCL']
            data_json['price_of_items_amt_vat_incl'] = transaction['PRICE_OF_ITEMS_AMT_VAT_INCL']
            data_json['promo_price_of_items_amt_vat_incl'] = transaction['PROMO_PRICE_OF_ITEMS_AMT_VAT_INCL']
            data_json['total_price_of_items_amt_vat_incl'] = transaction['TOTAL_PRICE_OF_ITEMS_AMT_VAT_INCL']
            data_json['promo_ship_charge_amt_vat_incl'] = transaction['PROMO_SHIP_CHARGE_AMT_VAT_INCL']
            data_json['total_ship_charge_amt_vat_incl'] = transaction['TOTAL_SHIP_CHARGE_AMT_VAT_INCL']
            data_json['transportation_mode'] = transaction['TRANSPORTATION_MODE']
            data_json['delivery_conditions'] = transaction['DELIVERY_CONDITIONS']
            data_json['vat_inv_currency_code'] = transaction['VAT_INV_CURRENCY_CODE']
            data_json['vat_inv_exchange_rate'] = transaction['VAT_INV_EXCHANGE_RATE']
            data_json['vat_inv_exchange_rate_date'] = transaction['VAT_INV_EXCHANGE_RATE_DATE']
            data_json['export_outside_eu'] = transaction['EXPORT_OUTSIDE_EU']
            data_json['invoice_url'] = transaction['INVOICE_URL']
            data_json['supplier_name'] = transaction['SUPPLIER_NAME']
            data_json['supplier_vat_number'] = transaction['SUPPLIER_VAT_NUMBER']
            t['data_json'] = data_json
            # taxable_jurisdiction = transaction['TAXABLE_JURISDICTION']
            # if taxable_jurisdiction:
            #     t['taxable_jurisdiction'] = transaction['TAXABLE_JURISDICTION']
            t['if_digital'] = False
            t['deemed'] = transaction.get('TAX_COLLECTION_RESPONSIBILITY', '') == 'MARKETPLACE'
            t['if_vat_calculate'] = True
            t['service_code'] = SERVICE_ID_GOODS
            t['reverse_charge_transaction_type_code'] = REVERSE_CHARGE_ID_GOODS
            if t['vat_number_of_buyer'] and t['vat_number_of_buyer'][0:2] != t['country_of_buyer']:
                t['vat_number_of_buyer'] = '{}{}'.format(t['country_of_buyer'], t['vat_number_of_buyer'])
            if t['seller_vat_number'] and t['seller_vat_number'][0:2] != t['country_of_report']:
                t['seller_vat_number'] = '{}{}'.format(t['country_of_report'], t['seller_vat_number'])
            list_transaction.append(t)
    return list_transaction


@user_login
async def integration(request, data):
    website = data['website']
    amazon_registration_email = data.get('website_email')
    good_or_service = data.get('good_or_service')
    company_id = request.company_user.company_id
    user_id = request.company_user.user_id
    auth_token = data.pop('auth_token')
    seller_id = data.pop('seller_id')
    marketplace_ids = None
    extra_data = {
        'auth_token': auth_token,
        'seller_id': seller_id,
        'amazon_registration_email': amazon_registration_email
    }
    # r = mws_reports(extra_data)
    # print(r)
    # if not r:
    #     raise HTTPBadRequest(reason='Incorrect Auth Token and Seller ID')
    if data.get('marketplace_ids'):
        marketplace_ids = data.pop('marketplace_ids')

    website_ = await request.app.m.website.get_one(request.app.m.website.table.c.website == website,
                                                  connection=request.connection, silent=True)

    if website_:
        if website_.platform or website_.company_id != company_id:
            await request.app.m.system_message.system_message_errors('sites', 'WRONG_COMPANY', company_id,
                                                                     data={'website_name': website_.website},
                                                                     connection=request.connection)
            raise web.HTTPFound('/sites/websites')
    else:
        company = await request.app.m.company.get_one(company_id, connection=request.connection)
        country_id = data.get('country_id', company.country_id)
        departure_country_id = data.get('departure_country_id')
        departure_state = data.get('departure_state')
        departure_zip = data.get('departure_zip')
        website_ = await request.app.m.website.create(
            website=website,
            website_url=website,
            currency='EUR',
            country_id=country_id,
            departure_state=departure_state,
            departure_zip=departure_zip,
            departure_country_id=departure_country_id,
            status=WebsiteStatus.active.value,
            company_id=request.company_user.company_id,
            connection=request.connection)
    async with request.connection.begin():
        await request.app.m.website_services.delete_where(
            request.app.m.website_services.table.c.website_id == website_.pk,
            connection=request.connection)
        await request.app.m.website_goods.delete_where(request.app.m.website_goods.table.c.website_id == website_.pk,
                                                       connection=request.connection)
        if good_or_service:
            for g_or_s in good_or_service:
                if g_or_s['type'] == 'good':
                    await request.app.m.website_goods.create(
                        website_id=website_.pk, code=g_or_s['code'], name=g_or_s['name'],
                        connection=request.connection)
                if g_or_s['type'] == 'service':
                    await request.app.m.website_services.create(
                        website_id=website_.pk, service_id=int(g_or_s['code']), description=g_or_s['name'],
                        connection=request.connection)

    if marketplace_ids:
        extra_data['marketplace_ids'] = marketplace_ids
    await request.app.m.user_social_auth.create(
        # uid=website,
        uid='website_id:{}'.format(website_.pk),
        extra_data=extra_data,
        provider='amazon',
        user_id=user_id,
        connection=request.connection)

    website_.platform = WebsitePlatform.amazon.value
    await website_.save(fields=['platform', ], connection=request.connection)
    return dict(status=200)
