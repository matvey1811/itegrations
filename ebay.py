import base64
import json
import urllib
from time import sleep

import pytz
import requests
from django.core.exceptions import ObjectDoesNotExist
from dvhb_hybrid import exceptions
from ebaysdk.trading import Connection as Trading
from ebaysdk.exception import ConnectionError
from social_django.models import UserSocialAuth

from lovat import users, companies
from lovat.auth import user_login
from aiohttp import web

from lovat.companies.enums import CompanyUserRole
from lovat.companies.models import CompanyUsers
from lovat.countries.models import Country
from lovat.currency.models import Currency
from lovat.notify.utils import system_message_errors
from lovat.shopify import shopify_redirect
from lovat.users.models import User
from lovat.websites.enums import WebsiteStatus, WebsitePlatform
from lovat.websites.models import Website
from lovat import settings
from django.db.models import Q

dev_id = settings.ebay.dev_id
client_id = settings.ebay.client_id
client_secret = settings.ebay.client_secret

ebay_domain = settings.ebay.ebay_domain
redirect_uri = settings.ebay.redirect_uri
from django_redis import get_redis_connection

scope='https://api.ebay.com/oauth/api_scope%20https://api.ebay.com/oauth/api_scope/sell.marketing.readonly%20https://api.ebay.com/oauth/api_scope/sell.marketing%20https://api.ebay.com/oauth/api_scope/sell.inventory.readonly%20https://api.ebay.com/oauth/api_scope/sell.inventory%20https://api.ebay.com/oauth/api_scope/sell.account.readonly%20https://api.ebay.com/oauth/api_scope/sell.account%20https://api.ebay.com/oauth/api_scope/sell.fulfillment.readonly%20https://api.ebay.com/oauth/api_scope/sell.fulfillment%20https://api.ebay.com/oauth/api_scope/sell.analytics.readonly%20https://api.ebay.com/oauth/api_scope/sell.finances%20https://api.ebay.com/oauth/api_scope/sell.payment.dispute%20https://api.ebay.com/oauth/api_scope/commerce.identity.readonly'


def get_ebay(request):
    code = request.GET['code']
    state = request.GET.get('state')
    token = get_auth_token(code)
    api = Trading(domain=ebay_domain, compatibility=str(967), appid=client_id, devid=dev_id, certid=client_secret,
                  config_file=None, iaf_token=token['access_token'])
    user = api.execute('GetUser', {}).dict()['User']
    need_auth, user = auth(request, user['UserID'], user['Email'], user['Site'], token, state)
    return shopify_redirect("/profile", user.id, need_auth)


def auth(request, user_id, email, country_code, token, state):
    from lovat.geo.plugins import GeoCoding
    need_auth = True
    apikey = request.COOKIES.get('api_key')
    r = get_redis_connection("default")
    data = r.hgetall("lovat:session:{}".format(apikey))
    data = {k.decode(): v.decode() for k, v in data.items()}
    if state:
        # state = state.replace('+', '"')
        # state = urllib.parse.unquote(state)
        # state = json.loads(state)
        # state = json.loads(state)
        state = state.replace('\\', '"')
        state = json.loads(state)
        company_user = CompanyUsers.objects.get(company_id=state[' company_id'], role=CompanyUserRole.owner.value)
        company = company_user.company
        user = company_user.user
        try:
            query = Q(code=country_code)
            query.add(Q(name=country_code), Q.OR)
            query.add(Q(full_name=country_code), Q.OR)
            query.add(Q(iso_code_2=country_code), Q.OR)
            country = Country.objects.filter(query).all()[:1].get()
        except ObjectDoesNotExist:
            country = None
        user_id = state[' website'][1:]
        need_auth = False
    else:
        try:
            user = User.objects.get(email=email, is_active=True)
        except ObjectDoesNotExist:
            if 'uid' in data:
                # we have already authenticated user
                need_auth = False
                uid = data['uid']
                try:
                    user = User.objects.get(id=uid, is_active=True)
                except ObjectDoesNotExist:
                    user = users.utils.get_or_add_user(email=email, first_name=' ', last_name=' ')
            else:
                user = users.utils.get_or_add_user(email=email, first_name=' ', last_name=' ')
        try:
            query = Q(code=country_code)
            query.add(Q(name=country_code), Q.OR)
            query.add(Q(full_name=country_code), Q.OR)
            query.add(Q(iso_code_2=country_code), Q.OR)
            country = Country.objects.filter(query).all()[:1].get()
        except ObjectDoesNotExist:
            country = None
        # t = pytz.country_timezones(country.iso_code_2)
        company = companies.utils.get_or_add_company(user_id, user, '', '', country.name, None, email)
        company_user = CompanyUsers.objects.get(company=company, role=CompanyUserRole.owner.value)

    try:
        website = Website.objects.get(website=user_id)
        if website.company != company:
            system_message_errors('sites', 'WRONG_COMPANY', company, data={'website_name': user_id})
        if website.platform != WebsitePlatform.ebay.value:
            website.platform = WebsitePlatform.ebay.value
            website.status = WebsiteStatus.active.value
            website.currency = Currency.objects.get(code='EUR')
            if country:
                website.country = country
                t = pytz.country_timezones(website.country.iso_code_2)
                website.timezone = t[0]
            website.save()
    except ObjectDoesNotExist:
        website = Website()
        website.company = company
        website.status = WebsiteStatus.active.value
        website.website = user_id
        website.currency = Currency.objects.get(code='EUR')
        if country:
            website.country = country
            t = pytz.country_timezones(website.country.iso_code_2)
            website.timezone = t[0]
        website.platform = WebsitePlatform.ebay.value
        website.save()

    try:
        user_social_auth = UserSocialAuth.objects.get(provider='ebay', uid='website_id:{}'.format(website.id))
        user_social_auth.extra_data = token
        user_social_auth.save()
    except ObjectDoesNotExist:
        user_social_auth = UserSocialAuth()
        user_social_auth.provider = 'ebay'
        user_social_auth.uid = 'website_id:{}'.format(website.id)
        user_social_auth.extra_data = token
        user_social_auth.user = company_user.user
        user_social_auth.save()
    return need_auth, user


def get_auth_token(code):
    basic = base64.b64encode('{}:{}'.format(client_id, client_secret).encode()).decode('utf-8')

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic {}".format(basic)
    }

    data = {
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
        "code": code
    }

    url = "https://{}/identity/v1/oauth2/token".format(ebay_domain)

    r = requests.post(url, headers=headers, data=data)
    return r.json()


def refresh_token(social):
    basic = base64.b64encode('{}:{}'.format(client_id, client_secret).encode()).decode('utf-8')

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic {}".format(basic)
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token": social.extra_data['refresh_token'],
    }

    url = "https://{}/identity/v1/oauth2/token".format(ebay_domain)

    r = requests.post(url, headers=headers, data=data)
    token = r.json()
    social.extra_data['access_token'] = token['access_token']
    social.save()


def connect(social):
    return Trading(domain=ebay_domain, compatibility=str(967), appid=client_id, devid=dev_id, certid=client_secret,
                   config_file=None, iaf_token=social.extra_data['access_token'])


def execute(api, verb, data, social, count=0):
    try:
        result = api.execute(verb, data)
        if result.reply.get('PaginationResult'):
            total_number_of_pages = int(result.reply.PaginationResult.TotalNumberOfPages)
        else:
            total_number_of_pages = 1
        if total_number_of_pages == 1:
            result = result.dict()
        else:
            result_ = list()
            result_.append(result.dict())
            if not data.get('Pagination'):
                data['Pagination'] = dict()
                data['Pagination']['PageNumber'] = 1
            for i in range(total_number_of_pages-1):
                data['Pagination']['PageNumber'] += 1
                result = api.execute(verb, data)
                result_.append(result.dict())
                sleep(2)
            result = result_
        return result
    except Exception as e:
        print(e)
        if 'Expired IAF token' in e.message:
            if count > 10:
                raise exceptions.HTTPBadRequest(reason=e.message)
            refresh_token(social)
            api = connect(social)
            count += 1
            return execute(api, verb, data, social, count)
        else:
            raise exceptions.HTTPBadRequest(reason=e.message)


@user_login
async def integration(request, data):
    website = data['website']
    good_or_service = data.get('good_or_service')
    company_id = request.company_user.company_id

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
            status=WebsiteStatus.active.value,
            country_id=country_id,
            departure_state=departure_state,
            departure_zip=departure_zip,
            departure_country_id=departure_country_id,
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
                    print(g_or_s)
                    await request.app.m.website_goods.create(
                        website_id=website_.pk, code=g_or_s['code'], name=g_or_s['name'],
                        connection=request.connection)
                if g_or_s['type'] == 'service':
                    await request.app.m.website_services.create(
                        website_id=website_.pk, service_id=int(g_or_s['code']), description=g_or_s['name'],
                        connection=request.connection)

    state = {'company_id': company_id, 'website': website}
    state = json.dumps(state)
    state = state.replace(' ', '')
    state = urllib.parse.quote(json.dumps(state))
    ebay_domain_ = ebay_domain.replace('api', 'auth')
    url = "https://{}/oauth2/consents?client_id={}&response_type=code&redirect_uri={}&scope={}&state={}".format(
        ebay_domain_, client_id, redirect_uri, scope, state)
    # raise web.HTTPFound(url)
    return url


def get_list_transaction(social, min_date, max_date):
    from lovat.services.utils import SERVICE_ID_GOODS
    import math
    from dateutil.relativedelta import relativedelta
    api = connect(social)
    list_transaction = list()
    t = math.ceil((max_date - min_date).days/30)
    for i in range(t):
        min_date_ = min_date + relativedelta(days=+ 29 * i)
        max_date_ = min_date_ + relativedelta(days=+ 29, hour=23, minute=59, second=59)
        if max_date_ > max_date:
            max_date_ = max_date
        tt = execute(api, 'GetSellerTransactions', {"ModTimeFrom": min_date_, "ModTimeTo": max_date_}, social)
        if type(tt) == dict:
            if not tt.get('TransactionArray'):
                continue
            transactions = tt['TransactionArray']['Transaction']
        else:
            transactions = list()
            for t in tt:
                if not t.get('TransactionArray'):
                    continue
                transactions += t['TransactionArray']['Transaction']
        for transaction in transactions:
            returns = ('ReturnClosedWithRefund', 'ReturnEscalatedClosedWithRefund', 'ReturnRequestClosedWithRefund', )
            if (transaction['Status']['CheckoutStatus'] == 'CheckoutComplete') or (transaction['Status']['ReturnStatus'] in returns):
                is_refund_transaction = transaction['Status'].get('ReturnStatus') in returns
                t = dict()
                t['transaction_id'] = transaction['TransactionID']
                t['transaction_datetime'] = transaction['Status']['LastTimeModified'] if transaction['Status'].get('LastTimeModified') else transaction['CreatedDate']
                transaction_sum = abs(float(transaction['AmountPaid']['value']))
                t['transaction_sum'] = -transaction_sum if is_refund_transaction else transaction_sum
                t['currency'] = transaction['AmountPaid']['_currencyID']
                t['transaction_status'] = 'refund' if is_refund_transaction else 'success'
                t['delivery_address'] = transaction['Buyer']['BuyerInfo']['ShippingAddress']['Country']
                t['arrival_country'] = transaction['Buyer']['BuyerInfo']['ShippingAddress']['Country']
                t['arrival_city'] = transaction['Buyer']['BuyerInfo']['ShippingAddress']['CityName']
                t['arrival_address_line'] = transaction['Buyer']['BuyerInfo']['ShippingAddress']['Street1']
                t['buyer_name'] = transaction['Buyer']['BuyerInfo']['ShippingAddress']['Name']
                t['if_digital'] = False
                t['if_vat_calculate'] = True
                t['service_code'] = SERVICE_ID_GOODS
                try:
                    if transaction.get('Taxes'):
                        t['deemed'] = transaction['Taxes']['TaxDetails']['Imposition'] == 'CustomCode'
                except:
                    pass

                try:
                    tt = execute(api, 'GetItem', {'ItemID': transaction['Item']['ItemID']}, social)
                    t['departure_address'] = tt['Item']['Country']
                except:
                    t['departure_address'] = transaction['Buyer']['BuyerInfo']['ShippingAddress']['Country']

                list_transaction.append(t)
    return list_transaction, 'Ok, {}'.format(len(list_transaction))


def send_anual_request_to_ebay_api(u):
    api = connect(u)
    execute(api, 'GetAccount', {}, u)
    return