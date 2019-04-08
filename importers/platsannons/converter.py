import logging
import re
from dateutil import parser
from importers.repository import taxonomy

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)

log = logging.getLogger(__name__)


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
    if 'annons' in message_envelope and 'version' in message_envelope:
        message = message_envelope['annons']
        annons = dict()
        annons['id'] = message.get('annonsId')
        annons['rubrik'] = message.get('annonsrubrik')
        annons['sista_ansokningsdatum'] = _isodate(message.get('sistaAnsokningsdatum'))
        annons['antal_platser'] = message.get('antalPlatser')
        annons['beskrivning'] = {
            'annonstext': message.get('annonstext'),
            'information': message.get('ftgInfo'),
            'behov': message.get('beskrivningBehov'),
            'krav': message.get('beskrivningKrav'),
            'villkor': message.get('villkorsbeskrivning'),
        }
        annons['arbetsplats_id'] = message.get('arbetsplatsId')
        annons['anstallningstyp'] = _expand_taxonomy_value('anstallningstyp',
                                                           'anstallningTyp',
                                                           message)
        annons['lonetyp'] = _expand_taxonomy_value('lonetyp', 'lonTyp', message)
        annons['varaktighet'] = _expand_taxonomy_value('varaktighet',
                                                       'varaktighetTyp', message)
        annons['arbetstidstyp'] = _expand_taxonomy_value('arbetstidstyp',
                                                         'arbetstidTyp', message)
        # If arbetstidstyp == 1 (heltid) then default omfattning == 100%
        default_omf = 100 if message.get('arbetstidTyp', {}).get('varde') == "1" else None
        annons['arbetsomfattning'] = {
            'min': message.get('arbetstidOmfattningFran', default_omf),
            'max': message.get('arbetstidOmfattningTill', default_omf)
        }
        annons['tilltrade'] = message.get('tilltrade')
        annons['arbetsgivare'] = {
            'id': message.get('arbetsgivareId'),
            'telefonnummer': message.get('telefonnummer'),
            'epost': message.get('epost'),
            'webbadress': message.get('webbadress'),
            'organisationsnummer': message.get('organisationsnummer'),
            'namn': message.get('arbetsgivareNamn'),
            'arbetsplats': message.get('arbetsplatsNamn')
        }
        annons['ansokningsdetaljer'] = {
            'information': message.get('informationAnsokningssatt'),
            'referens': message.get('referens'),
            'epost': message.get('ansokningssattEpost'),
            'via_af': message.get('ansokningssattViaAF'),
            'webbadress': message.get('ansokningssattWebbadress'),
            'annat': message.get('ansokningssattAnnatSatt')
        }
        annons['erfarenhet_kravs'] = not message.get('ingenErfarenhetKravs', False)
        annons['egen_bil'] = message.get('tillgangTillEgenBil', False)
        if message.get('korkort', []):
            annons['korkort_kravs'] = True
            annons['korkort'] = parse_driving_licence(message)
        else:
            annons['korkort_kravs'] = False
        if 'yrkesroll' in message:
            # jafhk fixa parsning för dessa med get_concept_by_legacy_id
            yrkesroll = taxonomy.get_concept_by_legacy_id('yrkesroll',
                                                          message.get('yrkesroll',
                                                                      {}).get('varde'))
            if yrkesroll and 'parent' in yrkesroll:
                yrkesgrupp = yrkesroll.get('parent')
                yrkesomrade = yrkesgrupp.get('parent')
                annons['yrkesroll'] = {'kod': yrkesroll['concept_id'],
                                       'term': yrkesroll['label'],
                                       'taxonomi-kod':
                                       yrkesroll['legacy_ams_taxonomy_id']}
                annons['yrkesgrupp'] = {'kod': yrkesgrupp['concept_id'],
                                        'term': yrkesgrupp['label'],
                                        'taxonomi-kod':
                                        yrkesgrupp['legacy_ams_taxonomy_id']}
                annons['yrkesomrade'] = {'kod': yrkesomrade['concept_id'],
                                         'term': yrkesomrade['label'],
                                         'taxonomi-kod':
                                         yrkesomrade['legacy_ams_taxonomy_id']}
            elif not yrkesroll:
                log.warning('Taxonomy value (1) not found for "yrkesroll" (%s)'
                            % message['yrkesroll'])
            else:  # yrkesroll is not None and 'parent' not in yrkesroll
                log.warning('Parent not found for yrkesroll %s (%s)'
                            % (message['yrkesroll'], yrkesroll))
        arbplatsmessage = message.get('arbetsplatsadress', {})
        kommun = None
        lansnamn = None
        kommunkod = None
        lanskod = None
        land = None
        landskod = None
        longitud = None
        latitud = None
        if 'kommun' in arbplatsmessage:
            kommunkod = arbplatsmessage.get('kommun', {}).get('varde', {})
            kommun = taxonomy.get_term('kommun', kommunkod)
        if 'lan' in arbplatsmessage:
            lanskod = arbplatsmessage.get('lan', {}).get('varde', {})
            lansnamn = taxonomy.get_term('lan', lanskod)
        if 'land' in arbplatsmessage:
            landskod = arbplatsmessage.get('land', {}).get('varde', {})
            land = taxonomy.get_term('land', landskod)
        if 'longitud'in arbplatsmessage:
            longitud = float(arbplatsmessage.get('longitud'))
        if 'latitud' in arbplatsmessage:
            latitud = float(arbplatsmessage.get('latitud'))

        annons['arbetsplatsadress'] = {
            'kommunkod': kommunkod,
            'lanskod': lanskod,
            'kommun': kommun,
            'lan': lansnamn,
            'landskod': landskod,
            'land': land,
            'gatuadress': message.get('besoksadress', {}).get('gatuadress'),
            'postnummer': message.get('postadress', {}).get('postnr'),
            'postort': message.get('postadress', {}).get('postort'),
            'coordinates': [longitud, latitud],
        }
        annons['krav'] = {
            'kompetenser': [
                get_concept_as_annons_value_with_weight('kompetens',
                                                        kompetens.get('varde'),
                                                        kompetens.get('vikt'))
                for kompetens in
                message.get('kompetenser', []) if kompetens.get('vikt', 0) > 3
            ],
            'sprak': [
                get_concept_as_annons_value_with_weight('sprak', sprak.get('varde'),
                                                        sprak.get('vikt'))
                for sprak in message.get('sprak', []) if sprak.get('vikt', 0) > 3
            ],
            'utbildningsniva': [
                get_concept_as_annons_value_with_weight('deprecated_educationlevel',
                                                        utbn.get('varde'),
                                                        utbn.get('vikt'))
                for utbn in
                [message.get('utbildningsniva', {})] if utbn.get('vikt', 0) > 3
            ],
            'utbildningsinriktning': [
                get_concept_as_annons_value_with_weight('deprecated_educationfield',
                                                        utbi.get('varde'),
                                                        utbi.get('vikt'))
                for utbi in
                [message.get('utbildningsinriktning', {})]
                if utbi and utbi.get('vikt', 0) > 3
            ],
            'yrkeserfarenheter': [
                get_concept_as_annons_value_with_weight('yrkesroll',
                                                        yrkerf.get('varde'),
                                                        yrkerf.get('vikt'))
                for yrkerf in
                message.get('yrkeserfarenheter', []) if yrkerf.get('vikt', 0) > 3
            ]
        }
        annons['meriterande'] = {
            'kompetenser': [
                get_concept_as_annons_value_with_weight('kompetens',
                                                        kompetens.get('varde'),
                                                        kompetens.get('vikt'))
                for kompetens in
                message.get('kompetenser', []) if kompetens.get('vikt', 0) < 4
            ],
            'sprak': [
                get_concept_as_annons_value_with_weight('sprak',
                                                        sprak.get('varde'),
                                                        sprak.get('vikt'))
                for sprak in message.get('sprak', []) if sprak.get('vikt', 0) < 4
            ],
            'utbildningsniva': [
                get_concept_as_annons_value_with_weight('deprecated_educationlevel',
                                                        utbn.get('varde'),
                                                        utbn.get('vikt'))
                for utbn in
                [message.get('utbildningsniva', {})] if utbn and utbn.get('vikt', 0) < 4
            ],
            'utbildningsinriktning': [
                get_concept_as_annons_value_with_weight('deprecated_educationfield',
                                                        utbi.get('varde'),
                                                        utbi.get('vikt'))
                for utbi in
                [message.get('utbildningsinriktning', {})]
                if utbi and utbi.get('vikt', 0) < 4  # hantera nullvärden
            ],
            'yrkeserfarenheter': [
                get_concept_as_annons_value_with_weight('yrkeserfarenheter',
                                                        yrkerf.get('varde'),
                                                        yrkerf.get('vikt'))
                for yrkerf in
                message.get('yrkeserfarenheter', []) if yrkerf.get('vikt', 0) < 4
            ]
        }
        annons['publiceringsdatum'] = _isodate(message.get('publiceringsdatum'))
        annons['kalla'] = message.get('kallaTyp')
        annons['publiceringskanaler'] = {
            'platsbanken': message.get('publiceringskanalPlatsbanken', False),
            'ais': message.get('publiceringskanalAis', False),
            'platsjournalen': message.get('publiceringskanalPlatsjournalen', False)
        }
        annons['status'] = {
            'publicerad': (message.get('status') == 'PUBLICERAD'
                           or message.get('status') == 'GODKAND_FOR_PUBLICERING'),
            'sista_publiceringsdatum': _isodate(message.get('sistaPubliceringsdatum')),
            'skapad': _isodate(message.get('skapadTid')),
            'skapad_av': message.get('skapadAv'),
            'uppdaterad': _isodate(message.get('uppdateradTid')),
            'uppdaterad_av': message.get('uppdateradAv'),
            'anvandarId': message.get('anvandarId')
        }
        # Extract labels as keywords for easier searching
        return _add_keywords(annons)
    else:
        # Message is already in correct format
        return message_envelope


def _expand_taxonomy_value(annons_key, message_key, message_dict):
    message_value = message_dict.get(message_key, {}).get('varde') \
        if message_dict else None
    if message_value:
        concept = taxonomy.get_concept_by_legacy_id(annons_key, message_value)
        return {
            "kod": concept['concept_id'],
            "term": concept['label'],
            "taxonomi-kod": concept['legacy_ams_taxonomy_id']
        } if concept else None
    return None


def get_concept_as_annons_value_with_weight(taxtype, legacy_id, weight):
    concept = taxonomy.get_concept_by_legacy_id(taxtype, legacy_id)
    weighted_concept = {
        'kod': None,
        'term': None,
        'vikt': None,
        'taxonomi-kod': None
    }
    try:
        weighted_concept['kod'] = concept.get('concept_id', None)
        weighted_concept['term'] = concept.get('label', None)
        weighted_concept['vikt'] = weight
        weighted_concept['taxonomi-kod'] = concept.get('legacy_ams_taxonomy_id', None)
    except AttributeError:
        log.warning('Taxonomy (3) value not found for {0} {1}'.format(taxtype, legacy_id))
    return weighted_concept


def parse_driving_licence(message):
    taxkorkortList = []
    for kkort in message.get('korkort'):
        taxkortkort = taxonomy.get_concept_by_legacy_id('korkort', kkort['varde'])
        if taxkortkort:
            taxkorkortList.append({
                "kod": taxkortkort['concept_id'],
                "term": taxkortkort['label']
            })
    return taxkorkortList


def _add_keywords(annons):
    if 'enriched' not in annons:
        annons['enriched'] = {'keywords': {}}
    for key_dict in [
        {
            'location':
                [
                    'arbetsplatsadress.postort',
                    'arbetsplatsadress.kommun',
                    'arbetsplatsadress.lan',
                    'arbetsplatsadress.land',
                ]
        }
    ]:
        field = list(key_dict.keys())[0]
        keywords = set()
        for key in list(key_dict.values())[0]:
            values = _get_nested_value(key, annons)
            for value in values:
                for kw in _extract_taxonomy_label(value):
                    keywords.add(kw)
        annons['enriched']['keywords'][field] = list(keywords)
    return annons


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
                values.append(item.get(keypath[i+1]))
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
