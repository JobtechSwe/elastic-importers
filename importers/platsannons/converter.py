import logging
import re
import time
from dateutil import parser

from importers import settings
from importers.repository import taxonomy
from elasticsearch.exceptions import RequestError
from importers.common import clean_html

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)
log = logging.getLogger(__name__)

MUST_HAVE_WEIGHT = 10
NICE_TO_HAVE_WEIGHT = 5


def _date_parser(input_date):
    date = parser.parse(input_date)
    return date.isoformat()


def _isodate(input_date):
    if not input_date:
        return None
    try:
        return _date_parser(input_date)
    except ValueError as e:
        log.error('Failed to parse %s as a valid date' % input_date, e)
        return None


def _is_ad_remote(desc, title):
    """
    returns True if at least one of the match phrases is found in description or title of an ad, else returns False
    """
    text_to_check = f"{desc} {title}".lower()
    return any(x in text_to_check for x in settings.REMOTE_MATCH_PHRASES)


def convert_ad(message, taxonomy_2):
    annons = dict()
    start_time = int(time.time() * 1000)
    if 'removed_ad_filter' in message:
        annons['removed_ad_filter'] = message.get('removed_ad_filter')

    annons['id'] = message.get('annonsId')
    annons['external_id'] = message.get('externtAnnonsId')
    annons['headline'] = message.get('annonsrubrik')
    annons['application_deadline'] = _isodate(message.get('sistaAnsokningsdatum'))
    annons['number_of_vacancies'] = message.get('antalPlatser')

    cleaned_description_text = clean_html(message.get('annonstextFormaterad'))
    if cleaned_description_text == '' and not message.get('avpublicerad'):
        log.warning(f"description.text is empty for ad id: {annons['id']}")

    if not message.get('avpublicerad'):
        annons['remote_work'] = _is_ad_remote(cleaned_description_text, annons['headline'])

    annons['description'] = {
        'text': cleaned_description_text,
        'text_formatted': message.get('annonstextFormaterad'),
        'company_information': message.get('ftgInfo'),
        'needs': message.get('beskrivningBehov'),
        'requirements': message.get('beskrivningKrav'),
        'conditions': message.get('villkorsbeskrivning'),
    }
    annons['employment_type'] = _expand_taxonomy_value('anstallningstyp',
                                                       'anstallningTyp',
                                                       message)
    annons['salary_type'] = _expand_taxonomy_value('lonetyp', 'lonTyp', message)
    annons['salary_description'] = message.get('lonebeskrivning')
    annons['duration'] = _expand_taxonomy_value('varaktighet',
                                                'varaktighetTyp', message)
    annons['working_hours_type'] = _expand_taxonomy_value('arbetstidstyp',
                                                          'arbetstidTyp', message)
    (default_min_omf,
     default_max_omf) = _get_default_scope_of_work(
        message.get('arbetstidTyp', {}).get('varde')
    )
    annons['scope_of_work'] = {
        'min': message.get('arbetstidOmfattningFran', default_min_omf),
        'max': message.get('arbetstidOmfattningTill', default_max_omf)
    }
    annons['access'] = message.get('tilltrade')
    annons['employer'] = {
        'phone_number': message.get('telefonnummer'),
        'email': message.get('epost'),
        'url': message.get('webbadress'),
        'organization_number': message.get('organisationsnummer'),
        'name': message.get('arbetsgivareNamn'),
        'workplace': message.get('arbetsplatsNamn'),
        'workplace_id': message.get('arbetsplatsId')
    }
    annons['application_details'] = {
        'information': message.get('informationAnsokningssatt'),
        'reference': message.get('referens'),
        'email': message.get('ansokningssattEpost'),
        'via_af': message.get('ansokningssattViaAF'),
        'url': message.get('ansokningssattWebbadress'),
        'other': message.get('ansokningssattAnnatSatt')
    }
    annons['experience_required'] = not message.get('ingenErfarenhetKravs', False)
    annons['access_to_own_car'] = message.get('tillgangTillEgenBil', False)
    if message.get('korkort', []):
        annons['driving_license_required'] = True
        annons['driving_license'] = parse_driving_licence(message)
    else:
        annons['driving_license_required'] = False

    _set_occupations(annons, message, taxonomy_2)

    annons['workplace_address'] = _build_wp_address(message.get('arbetsplatsadress', {}))

    annons['must_have'] = {
        'skills': [
            get_concept_as_annons_value_with_weight('kompetens',
                                                    kompetens.get('varde'),
                                                    kompetens.get('vikt'))
            for kompetens in message.get('kompetenser', [])
            if get_null_safe_value(kompetens, 'vikt', 0) >= MUST_HAVE_WEIGHT
        ],
        'languages': [
            get_concept_as_annons_value_with_weight('sprak', sprak.get('varde'),
                                                    sprak.get('vikt'))
            for sprak in message.get('sprak', [])
            if get_null_safe_value(sprak, 'vikt', 0) >= MUST_HAVE_WEIGHT
        ],
        'work_experiences': [
            get_concept_as_annons_value_with_weight('yrkesroll',
                                                    yrkerf.get('varde'),
                                                    yrkerf.get('vikt'))
            for yrkerf in message.get('yrkeserfarenheter', [])
            if get_null_safe_value(yrkerf, 'vikt', 0) >= MUST_HAVE_WEIGHT
        ],
        'education': [],
        'education_level': [],
    }

    # TODO: loop will be updated in future
    skills = annons['must_have'].get('skills', [])
    terms = []
    if skills:
        for skill in skills:
            concept_id = skill.get('concept_id')
            replaced_terms = _check_and_add_replace_concept_id(concept_id, taxonomy_2, annons['id'])
            for replaced_term in replaced_terms:
                terms.append(replaced_term)
    annons['must_have']['skills'] += terms

    annons['nice_to_have'] = {
        'skills': [
            get_concept_as_annons_value_with_weight('kompetens',
                                                    kompetens.get('varde'),
                                                    kompetens.get('vikt'))
            for kompetens in message.get('kompetenser', [])
            if get_null_safe_value(kompetens,
                                   'vikt',
                                   NICE_TO_HAVE_WEIGHT) < MUST_HAVE_WEIGHT
        ],
        'languages': [
            get_concept_as_annons_value_with_weight('sprak',
                                                    sprak.get('varde'),
                                                    sprak.get('vikt'))
            for sprak in message.get('sprak', [])
            if get_null_safe_value(sprak, 'vikt', NICE_TO_HAVE_WEIGHT) < MUST_HAVE_WEIGHT
        ],
        'work_experiences': [
            get_concept_as_annons_value_with_weight('yrkesroll',
                                                    yrkerf.get('varde'),
                                                    yrkerf.get('vikt'))
            for yrkerf in message.get('yrkeserfarenheter', [])
            if get_null_safe_value(yrkerf, 'vikt', NICE_TO_HAVE_WEIGHT) < MUST_HAVE_WEIGHT
        ],
        'education': [],
        'education_level': [],
    }

    # TODO: loop need to be updated in nice way
    skills = annons['nice_to_have'].get('skills', [])
    terms = []
    if skills:
        for skill in skills:
            concept_id = skill.get('concept_id')
            replaced_terms = _check_and_add_replace_concept_id(concept_id, taxonomy_2, annons['id'])
            for replaced_term in replaced_terms:
                terms.append(replaced_term)
    annons['nice_to_have']['skills'] += terms

    annons['application_contact'] = _build_contacts(message.get('kontaktpersoner', []))

    if message.get('utbildningsinriktning'):
        try:
            _set_education(annons, message)
        except TypeError as e:
            log.warning(f"Skipping education on ad: {annons['id']} due to TypeError: {e}")

    annons['publication_date'] = _isodate(message.get('publiceringsdatum'))
    annons['last_publication_date'] = _isodate(message.get('sistaPubliceringsdatum'))
    annons['removed'] = message.get('avpublicerad')
    annons['removed_date'] = _isodate(message.get('avpubliceringsdatum'))
    annons['source_type'] = message.get('kallaTyp')
    annons['timestamp'] = message.get('updatedAt')
    annons['logo_url'] = message.get('logo_url')
    # Extract labels as keywords for easier searching
    log.debug("Convert ad: %s took: %d milliseconds." % (annons['id'], int(time.time() * 1000) - start_time))
    return _add_keywords(annons)


def _set_education(annons, message):
    if get_null_safe_value(message.get('utbildningsinriktning'),
                           'vikt', 0) >= MUST_HAVE_WEIGHT:
        annons['must_have']['education'] = [
            get_concept_as_annons_value_with_weight(
                ['sun-education-field-1', 'sun-education-field-2',
                 'sun-education-field-3'],
                message.get('utbildningsinriktning', {}).get('varde'),
                message.get('utbildningsinriktning', {}).get('vikt'))]
        annons['must_have']['education_level'] = [
            get_concept_as_annons_value_with_weight(
                ['sun-education-level-1', 'sun-education-level-2',
                 'sun-education-level-3'],
                message.get('utbildningsniva', {}).get('varde'),
                message.get('utbildningsniva', {}).get('vikt'))]
    else:
        annons['nice_to_have']['education'] = [
            get_concept_as_annons_value_with_weight(
                ['sun-education-field-1', 'sun-education-field-2',
                 'sun-education-field-3'],
                message.get('utbildningsinriktning', {}).get('varde'),
                message.get('utbildningsinriktning', {}).get('vikt'))]
        annons['nice_to_have']['education_level'] = [
            get_concept_as_annons_value_with_weight(
                ['sun-education-level-1', 'sun-education-level-2',
                 'sun-education-level-3'],
                message.get('utbildningsniva', {}).get('varde'),
                message.get('utbildningsniva', {}).get('vikt'))]


def _get_default_scope_of_work(arbtid_typ):
    default_max_omf = None
    default_min_omf = None

    # If heltid...
    if arbtid_typ == "6YE1_gAC_R2G":
        default_min_omf = 100
        default_max_omf = 100
    # Elif deltid or heltid/deltid
    elif arbtid_typ == "947z_JGS_Uk2" or arbtid_typ == "hGCt_6Ni_XPz":
        default_min_omf = 0
        default_max_omf = 100

    return default_min_omf, default_max_omf


def _build_contacts(kontaktpersoner):
    appl_contact_list = []
    for kontaktperson in kontaktpersoner:
        appl_contact = {}
        name = "%s %s" % (get_null_safe_value(kontaktperson, 'fornamn', ''),
                          get_null_safe_value(kontaktperson, 'efternamn', ''))
        appl_contact['name'] = name.strip() or None
        appl_contact['description'] = kontaktperson.get('beskrivning')
        appl_contact['email'] = kontaktperson.get('epost')
        phone_numbers = "%s, %s" % (get_null_safe_value(kontaktperson, 'telefonnummer', ''),
                                    get_null_safe_value(kontaktperson, 'mobilnummer', ''))
        appl_contact['telephone'] = phone_numbers.strip(', ') or None
        if kontaktperson.get('fackligRepresentant'):
            appl_contact['contactType'] = "Facklig representant"
        else:
            appl_contact['contactType'] = kontaktperson.get('befattning')

        appl_contact_list.append(appl_contact)

    return appl_contact_list


def _set_occupations(annons, message, taxonomy_2):
    if 'yrkesroll' in message:
        yrkesroll = taxonomy.get_legacy_by_concept_id('yrkesroll', message.get('yrkesroll', {}).get('varde'))

        if yrkesroll and 'parent' in yrkesroll:
            yrkesgrupp = yrkesroll.get('parent')
            yrkesomrade = yrkesgrupp.get('parent')
            annons['occupation'] = [{'concept_id': yrkesroll['concept_id'],
                                    'label': yrkesroll['label'],
                                     'legacy_ams_taxonomy_id':
                                         yrkesroll['legacy_ams_taxonomy_id']}]
            replaced_terms = _check_and_add_replace_concept_id(yrkesroll['concept_id'], taxonomy_2, annons['id'])
            if replaced_terms:
                for replaced_term in replaced_terms:
                    annons['occupation'].append({
                        'concept_id': replaced_term['concept_id'],
                        'label': replaced_term['label'],
                        'legacy_ams_taxonomy_id': replaced_term['legacy_ams_taxonomy_id']})
                    if replaced_term.get('new_version', False):
                        replaced_yrkesroll = taxonomy.get_legacy_by_concept_id('yrkesroll', replaced_term.get('concept_id'))
                        if replaced_yrkesroll and 'parent' in replaced_yrkesroll:
                            yrkesgrupp = replaced_yrkesroll.get('parent', {})
                            yrkesomrade = yrkesgrupp.get('parent')

            annons['occupation_group'] = [{'concept_id': yrkesgrupp['concept_id'],
                                          'label': yrkesgrupp['label'],
                                           'legacy_ams_taxonomy_id':
                                               yrkesgrupp['legacy_ams_taxonomy_id']}]
            annons['occupation_field'] = [{'concept_id': yrkesomrade['concept_id'],
                                          'label': yrkesomrade['label'],
                                           'legacy_ams_taxonomy_id':
                                               yrkesomrade['legacy_ams_taxonomy_id']}]
        elif not yrkesroll:
            log.warning(f"Taxonomy value not found for: {message['yrkesroll']}")
        else:  # yrkesroll is not None and 'parent' not in yrkesroll
            log.warning(f"Parent not found for yrkesroll: {message['yrkesroll']} ({yrkesroll})")


def _check_and_add_replace_concept_id(old_term_concept_id, taxonomy_2, annons_id):
    taxonomy_values = []
    for item in taxonomy_2:
        content = item.get("taxonomy/concept", {})
        internal_content = content.get("taxonomy/replaced-by", {})[0]
        if old_term_concept_id == content.get("taxonomy/id"):
            replaced_term = content.get("taxonomy/replaced-by")[0]
            log.info(f"Old taxonomy term: {old_term_concept_id} is replaced by: {replaced_term} in id: {annons_id}")
            taxonomy_values.append({
                'concept_id': replaced_term.get("taxonomy/id"),
                'label': replaced_term.get('taxonomy/preferred-label'),
                'legacy_ams_taxonomy_id': None,
                'new_version': True})
        elif old_term_concept_id == internal_content.get("taxonomy/id"):
            taxonomy_values.append({
                'concept_id': content.get("taxonomy/id"),
                'label': content.get('taxonomy/preferred-label'),
                'legacy_ams_taxonomy_id': None})
    return taxonomy_values


def _build_wp_address(arbplatsmessage):
    kommun = None
    lansnamn = None
    kommun_concept_id = None
    kommun_legacy_id = None
    land = None
    lan_legacy_id = None
    lan_concept_id = None
    land_legacy_id = None
    land_concept_id = None
    longitud = None
    latitud = None
    if 'kommun' in arbplatsmessage and arbplatsmessage.get('kommun'):
        kommun_concept_id = arbplatsmessage.get('kommun', {}).get('varde', {})
        kommun_temp = taxonomy.get_legacy_by_concept_id('kommun', kommun_concept_id)
        kommun_legacy_id = kommun_temp.get('legacy_ams_taxonomy_id', None)
        kommun = arbplatsmessage.get('kommun', {}).get('namn', {})
    if 'lan' in arbplatsmessage and arbplatsmessage.get('lan'):
        lan_concept_id = arbplatsmessage.get('lan', {}).get('varde', {})
        lan_temp = taxonomy.get_legacy_by_concept_id('lan', lan_concept_id)
        lan_legacy_id = lan_temp.get('legacy_ams_taxonomy_id', None)
        lansnamn = arbplatsmessage.get('lan', {}).get('namn', {})
    if 'land' in arbplatsmessage and arbplatsmessage.get('land'):
        land_concept_id = arbplatsmessage.get('land', {}).get('varde', {})
        land_temp = taxonomy.get_legacy_by_concept_id('land', land_concept_id)
        land_legacy_id = land_temp.get('legacy_ams_taxonomy_id', None)
        land = arbplatsmessage.get('land', {}).get('namn', {})
    if 'longitud' in arbplatsmessage and arbplatsmessage.get('longitud'):
        longitud = float(arbplatsmessage.get('longitud'))
    if 'latitud' in arbplatsmessage and arbplatsmessage.get('latitud'):
        latitud = float(arbplatsmessage.get('latitud'))

    return {
        'municipality_code': kommun_legacy_id,
        'municipality_concept_id': kommun_concept_id,
        'municipality': kommun,
        'region_code': lan_legacy_id,
        'region_concept_id': lan_concept_id,
        'region': lansnamn,
        'country_code': land_legacy_id or "199",
        'country_concept_id': land_concept_id or "i46j_HmG_v64",
        'country': land or "Sverige",
        # 'street_address': message.get('besoksadress', {}).get('gatuadress'),
        'street_address': arbplatsmessage.get('gatuadress', ''),
        # 'postcode': message.get('postadress', {}).get('postnr'),
        'postcode': arbplatsmessage.get('postnr'),
        # 'city': message.get('postadress', {}).get('postort'),
        'city': arbplatsmessage.get('postort'),
        'coordinates': [longitud, latitud]
    }


def get_null_safe_value(element, key, replacement_val):
    val = element.get(key, replacement_val)
    if val is None:
        val = replacement_val
    return val


def _expand_taxonomy_value(annons_key, message_key, message_dict):
    message_value = message_dict.get(message_key, {}).get('varde') if message_dict else None
    if message_value:
        concept = taxonomy.get_legacy_by_concept_id(annons_key, message_value)
        return {
            "concept_id": concept.get('concept_id', None),
            "label": concept.get('label', None),
            "legacy_ams_taxonomy_id": concept.get('legacy_ams_taxonomy_id', None)
        } if concept else None
    return None


def get_concept_as_annons_value_with_weight(taxtype, id, weight=None):
    weighted_concept = {
        'concept_id': None,
        'label': None,
        'weight': None,
        'legacy_ams_taxonomy_id': None
    }
    try:
        concept = taxonomy.get_legacy_by_concept_id(taxtype, id)
        weighted_concept['concept_id'] = concept.get('concept_id', None)
        weighted_concept['label'] = concept.get('label', None)
        weighted_concept['weight'] = weight
        weighted_concept['legacy_ams_taxonomy_id'] = concept.get('legacy_ams_taxonomy_id', None)
    except AttributeError:
        log.warning('Taxonomy (3) value not found for {0} {1}'.format(taxtype, id))
    except RequestError:
        log.warning(f'Request failed with argtype: {taxtype} and legacy_id or concept_id: {id}')
    return weighted_concept


def parse_driving_licence(message):
    taxkorkort_list = []
    for kkort in message.get('korkort'):
        taxkorkort = taxonomy.get_legacy_by_concept_id('korkort', kkort['varde'])
        if taxkorkort:
            taxkorkort_list.append({
                "concept_id": taxkorkort.get('concept_id', None),
                "legacy_ams_taxonomy_id": taxkorkort.get('legacy_ams_taxonomy_id', None),
                "label": taxkorkort.get('label', None)
            })
    return taxkorkort_list


def _add_keywords(annons):
    if 'keywords' not in annons:
        annons['keywords'] = {'extracted': {}}
    for key_dict in [
        {
            'occupation': [
                'occupation.label',
                'occupation_group.label',
                'occupation_field.label',
            ]
        },
        {
            'skill': [
                'must_have.skills.label',
                'must_have.languages.label',
                'nice_to_have.skills.label',
                'nice_to_have.languages.label',
            ]
        },
        {
            'location': [
                'workplace_address.city',
                'workplace_address.municipality',
                'workplace_address.region',
                'workplace_address.country',
            ]
        },
        {
            'employer': [
                'employer.name',
                'employer.workplace'
            ]
        }
    ]:
        field = list(key_dict.keys())[0]
        keywords = set()
        values = []
        for key in list(key_dict.values())[0]:
            values += _get_nested_value(key, annons)
        if field == 'employer':
            for value in _create_employer_name_keywords(values):
                keywords.add(value)
        elif field == 'location':
            for value in values:
                trimmed = _trim_location(value)
                if trimmed:
                    keywords.add(trimmed)
        else:
            for value in values:
                for kw in _extract_taxonomy_label(value):
                    keywords.add(kw)
        annons['keywords']['extracted'][field] = list(keywords)
    return annons


def _create_employer_name_keywords(companynames):
    names = []
    for companyname in companynames or []:
        converted_name = companyname.lower().strip()
        converted_name = __rightreplace(converted_name, ' ab', '')
        converted_name = __leftreplace(converted_name, 'ab ', '')
        names.append(converted_name)

    if names:
        names.sort(key=lambda s: len(s))
        shortest = len(names[0])
        uniques = [names[0]]
        for i in range(1, len(names)):
            if names[i][0:shortest] != names[0] and names[i]:
                uniques.append(names[i])

        return uniques

    return []


def __rightreplace(astring, pattern, sub):
    return sub.join(astring.rsplit(pattern, 1))


def __leftreplace(astring, pattern, sub):
    return sub.join(astring.split(pattern, 1))


def _trim_location(locationstring):
    # Look for unwanted words (see tests/unit/test_converter.py)
    pattern = '[0-9\\-]+|.+,|([\\d\\w]+\\-[\\d]+)|\\(.*|.*\\)|\\(\\)|\\w*\\d+\\w*'
    regex = re.compile(pattern)
    stopwords = ['box']
    if locationstring:
        # Magic regex
        valid_words = []
        for word in locationstring.lower().split():
            if word and not re.match(regex, word) and word not in stopwords:
                valid_words.append(word)
        return ' '.join(valid_words)
    return locationstring


def _get_nested_value(path, data):
    keypath = path.split('.')
    values = []
    for i in range(len(keypath)):
        element = data.get(keypath[i])
        if isinstance(element, str):
            values.append(element)
            break
        if isinstance(element, list):
            for item in element:
                if item:
                    values.append(item.get(keypath[i + 1]))
            break
        if isinstance(element, dict):
            data = element
    return values


def _extract_taxonomy_label(label):
    if not label:
        return []
    try:
        label = label.replace('m.fl.', '').strip()
        if '-' in label:
            return [word.lower() for word in re.split(r'/', label)]
        else:
            return [word.lower().strip() for word in re.split(r'/|, | och ', label)]
    except AttributeError:
        log.warning('(extract_taxonomy_label) extract fail for: %s' % label)
    return []
