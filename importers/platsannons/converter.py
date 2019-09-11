import logging
import re
from dateutil import parser
from importers.repository import taxonomy
from elasticsearch.exceptions import RequestError

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)

log = logging.getLogger(__name__)

MUST_HAVE_WEIGHT = 10


def _isodate(bad_date):
    if not bad_date:
        return None
    try:
        date = parser.parse(bad_date)
        return date.isoformat()
    except ValueError as e:
        log.error('Failed to parse %s as a valid date' % bad_date, e)
        return None


def convert_message(message_envelope):
    if 'version' in message_envelope:
        message = message_envelope
        annons = dict()
        annons['id'] = message.get('annonsId')
        annons['external_id'] = message.get('externtAnnonsId')
        annons['headline'] = message.get('annonsrubrik')
        annons['application_deadline'] = _isodate(message.get('sistaAnsokningsdatum'))
        annons['number_of_vacancies'] = message.get('antalPlatser')
        annons['description'] = {
            'text': message.get('annonstext'),
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

        _set_occupations(annons, message)

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

        annons['nice_to_have'] = {
            'skills': [
                get_concept_as_annons_value_with_weight('kompetens',
                                                        kompetens.get('varde'),
                                                        kompetens.get('vikt'))
                for kompetens in message.get('kompetenser', [])
                if get_null_safe_value(kompetens, 'vikt', MUST_HAVE_WEIGHT) < MUST_HAVE_WEIGHT
            ],
            'languages': [
                get_concept_as_annons_value_with_weight('sprak',
                                                        sprak.get('varde'),
                                                        sprak.get('vikt'))
                for sprak in message.get('sprak', [])
                if get_null_safe_value(sprak, 'vikt', MUST_HAVE_WEIGHT) < MUST_HAVE_WEIGHT
            ],
            'work_experiences': [
                get_concept_as_annons_value_with_weight('yrkesroll',
                                                        yrkerf.get('varde'),
                                                        yrkerf.get('vikt'))
                for yrkerf in message.get('yrkeserfarenheter', [])
                if get_null_safe_value(yrkerf, 'vikt', MUST_HAVE_WEIGHT) < MUST_HAVE_WEIGHT
            ],
            'education': [],
            'education_level': [],
        }

        if message.get('utbildningsinriktning'):
            try:
                _set_education(annons, message)
            except TypeError:
                log.warning(f"Skipping education fields on ad {annons['id']} due to TypeError")

        annons['publication_date'] = _isodate(message.get('publiceringsdatum'))
        annons['last_publication_date'] = _isodate(message.get('sistaPubliceringsdatum'))
        annons['removed'] = message.get('avpublicerad')
        annons['removed_date'] = _isodate(message.get('avpubliceringsdatum'))
        annons['source_type'] = message.get('kallaTyp')
        # Extract labels as keywords for easier searching
        return _add_keywords(annons)
    else:
        # Message is already in correct format
        return message_envelope


def _set_education(annons, message):
    if get_null_safe_value(message.get('utbildningsinriktning'), 'vikt', 0) >= MUST_HAVE_WEIGHT:
        annons['must_have']['education'] = [
            get_concept_as_annons_value_with_weight(
                ['sun-education-field-1', 'sun-education-field-2', 'sun-education-field-3'],
                message.get('utbildningsinriktning', {}).get('varde'),
                message.get('utbildningsinriktning', {}).get('vikt'))]
        annons['must_have']['education_level'] = [
            get_concept_as_annons_value_with_weight(
                ['sun-education-level-1', 'sun-education-level-2', 'sun-education-level-3'],
                message.get('utbildningsniva', {}).get('varde'),
                message.get('utbildningsniva', {}).get('vikt'))]
    else:
        annons['nice_to_have']['education'] = [
            get_concept_as_annons_value_with_weight(
                ['sun-education-field-1', 'sun-education-field-2', 'sun-education-field-3'],
                message.get('utbildningsinriktning', {}).get('varde'),
                message.get('utbildningsinriktning', {}).get('vikt'))]
        annons['nice_to_have']['education_level'] = [
            get_concept_as_annons_value_with_weight(
                ['sun-education-level-1', 'sun-education-level-2', 'sun-education-level-3'],
                message.get('utbildningsniva', {}).get('varde'),
                message.get('utbildningsniva', {}).get('vikt'))]


def _get_default_scope_of_work(arbtid_typ):
    default_max_omf = None
    default_min_omf = None
    if arbtid_typ == "1":
        default_min_omf = 100
        default_max_omf = 100
    elif arbtid_typ == "2" or arbtid_typ == "3":
        default_min_omf = 0
        default_max_omf = 100
    return default_min_omf, default_max_omf


def _set_occupations(annons, message):
    if 'yrkesroll' in message:
        # jafhk fixa parsning för dessa med get_concept_by_legacy_id
        yrkesroll = taxonomy.get_concept_by_legacy_id('yrkesroll',
                                                      message.get('yrkesroll',
                                                                  {}).get('varde'))
        if yrkesroll and 'parent' in yrkesroll:
            yrkesgrupp = yrkesroll.get('parent')
            yrkesomrade = yrkesgrupp.get('parent')
            annons['occupation'] = {'concept_id': yrkesroll['concept_id'],
                                    'label': yrkesroll['label'],
                                    'legacy_ams_taxonomy_id':
                                        yrkesroll['legacy_ams_taxonomy_id']}
            annons['occupation_group'] = {'concept_id': yrkesgrupp['concept_id'],
                                          'label': yrkesgrupp['label'],
                                          'legacy_ams_taxonomy_id':
                                              yrkesgrupp['legacy_ams_taxonomy_id']}
            annons['occupation_field'] = {'concept_id': yrkesomrade['concept_id'],
                                          'label': yrkesomrade['label'],
                                          'legacy_ams_taxonomy_id':
                                              yrkesomrade['legacy_ams_taxonomy_id']}
        elif not yrkesroll:
            log.warning('Taxonomy value (1) not found for "yrkesroll" (%s)'
                        % message['yrkesroll'])
        else:  # yrkesroll is not None and 'parent' not in yrkesroll
            log.warning('Parent not found for yrkesroll %s (%s)' %
                        (message['yrkesroll'], yrkesroll))


def _build_wp_address(arbplatsmessage):
    kommun = None
    lansnamn = None
    kommunkod = None
    kommun_concept_id = None
    lanskod = None
    lan_concept_id = None
    land = None
    land_concept_id = None
    landskod = None
    longitud = None
    latitud = None
    if 'kommun' in arbplatsmessage and arbplatsmessage.get('kommun'):
        kommunkod = arbplatsmessage.get('kommun', {}).get('varde', {})
        kommun_concept_id = taxonomy.get_concept_by_legacy_id('kommun',
                                                              kommunkod)['concept_id']
        kommun = arbplatsmessage.get('kommun', {}).get('namn', {})
    if 'lan' in arbplatsmessage and arbplatsmessage.get('lan'):
        lanskod = arbplatsmessage.get('lan', {}).get('varde', {})
        lan_temp = taxonomy.get_concept_by_legacy_id('lan', lanskod)
        if 'concept_id' in lan_temp:
            lan_concept_id = lan_temp['concept_id']
        lansnamn = arbplatsmessage.get('lan', {}).get('namn', {})
    if 'land' in arbplatsmessage and arbplatsmessage.get('land'):
        landskod = arbplatsmessage.get('land', {}).get('varde', {})
        land_temp = taxonomy.get_concept_by_legacy_id('land', landskod)
        if 'concept_id' in land_temp:
            land_concept_id = land_temp['concept_id']
        land = arbplatsmessage.get('land', {}).get('namn', {})
    if 'longitud' in arbplatsmessage and arbplatsmessage.get('longitud'):
        longitud = float(arbplatsmessage.get('longitud'))
    if 'latitud' in arbplatsmessage and arbplatsmessage.get('latitud'):
        latitud = float(arbplatsmessage.get('latitud'))

    return {
        'municipality_code': kommunkod,
        'municipality_concept_id': kommun_concept_id,
        'municipality': kommun,
        'region_code': lanskod,
        'region_concept_id': lan_concept_id,
        'region': lansnamn,
        'country_code': landskod,
        'country_concept_id': land_concept_id,
        'country': land,
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
    message_value = message_dict.get(message_key, {}).get('varde') \
        if message_dict else None
    if message_value:
        concept = taxonomy.get_concept_by_legacy_id(annons_key, message_value)
        return {
            "concept_id": concept['concept_id'],
            "label": concept['label'],
            "legacy_ams_taxonomy_id": concept['legacy_ams_taxonomy_id']
        } if concept else None
    return None


def get_concept_as_annons_value_with_weight(taxtype, legacy_id, weight=None):
    weighted_concept = {
        'concept_id': None,
        'label': None,
        'weight': None,
        'legacy_ams_taxonomy_id': None
    }
    try:
        concept = taxonomy.get_concept_by_legacy_id(taxtype, legacy_id)
        weighted_concept['concept_id'] = concept.get('concept_id', None)
        weighted_concept['label'] = concept.get('label', None)
        weighted_concept['weight'] = weight
        weighted_concept['legacy_ams_taxonomy_id'] = concept.get('legacy_ams_taxonomy_id',
                                                                 None)
    except AttributeError:
        log.warning('Taxonomy (3) value not found for {0} {1}'.format(taxtype, legacy_id))
    except RequestError:
        log.warning(f'Taxonomy request failed with arguments type: {taxtype} and legacy_id: {legacy_id}')
    return weighted_concept


def parse_driving_licence(message):
    taxkorkort_list = []
    for kkort in message.get('korkort'):
        taxkortkort = taxonomy.get_concept_by_legacy_id('korkort', kkort['varde'])
        if taxkortkort:
            taxkorkort_list.append({
                "concept_id": taxkortkort['concept_id'],
                "label": taxkortkort['label']
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
    regex = re.compile('[0-9\\-]+|.+,|([\\d\\w]+\\-[\\d]+)|\\(.*|.*\\)|\\(\\)|\\w*\\d+\\w*')
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
        print('extract fail (%s)' % label)
    return []
