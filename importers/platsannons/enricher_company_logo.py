import requests
from importers import settings

# Cache for this import
existing_logos_org_nr = set()


def enrich(annonser):
    for annons in annonser:
        if 'employer' in annons:
            if 'organization_number' in annons['employer'] and annons['employer']['organization_number']:

                org_number = annons['employer']['organization_number']
                # AG-api:et
                eventual_logo_url = '%s%s/logotyper/logo.png' % (settings.COMPANY_LOGO_BASE_URL, org_number)

                # Legacy Open API:
                # For example http://api.arbetsformedlingen.se/platsannons/8394494/logotyp
                # eventual_logo_url = '%s%s/logotyp' % (settings.COMPANY_LOGO_BASE_URL, annons['id'])

                # INFO: To check if the logofile exists with http-HEAD: 1000 requests take about 120 sec
                # (5-6 times slower than without the requests).

                if settings.CHECK_LOGO_FILE_EXISTS:
                    if org_number in existing_logos_org_nr:
                        logo_url = eventual_logo_url
                    else:
                        r = requests.head(eventual_logo_url, timeout=15)
                        if r.status_code == 200:
                            logo_url = eventual_logo_url
                            existing_logos_org_nr.add(org_number)
                        else:
                            logo_url = None
                else:
                    logo_url = eventual_logo_url

                annons['employer']['logo_url'] = logo_url

    return annonser