import bs4
from importers import settings
from importers.repository.taxonomy import get_info_by_city_name


def convert_ad_onepartnergroup(message, url):
    annons = dict()
    message_soup = bs4.BeautifulSoup(message.text, 'lxml')
    annons['id'] = settings.participants_ids['onepartnergroup'] + message_soup.find(id='job-id').getText()
    annons['headline'] = message_soup.select('title')[0].getText()
    annons['occupation'] = {'label': message_soup.find(id='category').getText()}
    annons['description'] = {
        'text': message_soup.find(class_='content-wrapper rubrik_content').getText(),
        'text_formatted': str(message_soup.find(class_='content-wrapper rubrik_content')),
    }
    city_name = message_soup.find(id='city').getText()
    annons['workplace_address'] = use_city_info_get_workplace_address_info_from_taxonomy(city_name)
    annons['url'] = url
    return annons


def use_city_info_get_workplace_address_info_from_taxonomy(city):
    value = get_info_by_city_name(city)
    workplace_address = {}
    if value:
        workplace_address = {
            'municipality_code': value.get('legacy_ams_taxonomy_id', ''),
            'municipality_concept_id': value.get('concept_id', ''),
            'municipality': value.get('label', ''),
            'region_code': value.get('parent', '').get('legacy_ams_taxonomy_id', ''),
            'region_concept_id': value.get('parent', '').get('concept_id', ''),
            'region': value.get('parent', '').get('label', ''),
            'country_code': "199",
            'country_concept_id': "i46j_HmG_v64",
            'country': "Sverige",
        }
    return workplace_address
