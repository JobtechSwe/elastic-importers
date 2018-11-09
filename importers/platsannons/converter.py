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
            taxkorkort = []
            for kkort in message.get('korkort'):
                taxkorkort.append({
                    "kod": kkort['varde'],
                    "term": taxonomy.get_term('korkort', kkort['varde'])
                })
            annons['korkort'] = taxkorkort
        else:
            annons['korkort_kravs'] = False
        if 'yrkesroll' in message:
            yrkesroll = taxonomy.get_entity('yrkesroll',
                                            message.get('yrkesroll', {}).get('varde'))
            if yrkesroll and 'parent' in yrkesroll:
                yrkesgrupp = yrkesroll.pop('parent')
                yrkesomrade = yrkesgrupp.pop('parent')
                annons['yrkesroll'] = {'kod': yrkesroll['id'],
                                       'term': yrkesroll['label']}
                annons['yrkesgrupp'] = {'kod': yrkesgrupp['id'],
                                        'term': yrkesgrupp['label']}
                annons['yrkesomrade'] = {'kod': yrkesomrade['id'],
                                         'term': yrkesomrade['label']}
            elif not yrkesroll:
                log.warning('Taxonomy value not found for "yrkesroll" (%s)'
                            % message['yrkesroll'])
            else:  # yrkesroll is not None and 'parent' not in yrkesroll
                log.warning('Taxonomy value not found for "yrkesroll" (%s)'
                            % message['yrkesroll'])
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
                {'kod': kompetens.get('varde'),
                 'term': taxonomy.get_term('kompetens', kompetens.get('varde')),
                 'vikt': kompetens.get('vikt')
                 }
                for kompetens in
                message.get('kompetenser', []) if kompetens.get('vikt', 0) > 3
            ],
            'sprak': [
                {'kod': sprak.get('varde'),
                 'term': taxonomy.get_term('sprak', sprak.get('varde')),
                 'vikt': sprak.get('vikt')
                 }
                for sprak in message.get('sprak', []) if sprak.get('vikt', 0) > 3
            ],
            'utbildningsniva': [
                {'kod': utbn.get('varde'),
                 'term': taxonomy.get_term('utbildningsniva', utbn.get('varde')),
                 'vikt': utbn.get('vikt')
                 }
                for utbn in
                [message.get('utbildningsniva', {})] if utbn.get('vikt', 0) > 3

            ],
            'utbildningsinriktning': [
                {'kod': utbi.get('varde'),
                 'term': taxonomy.get_term('utbildningsinriktning', utbi.get('varde')),
                 'vikt': utbi.get('vikt')
                 }
                for utbi in
                [message.get('utbildningsinriktning', {})] if utbi.get('vikt', 0) > 3
            ],
            'yrkeserfarenheter': [
                {'kod': yrkerf.get('varde'),
                 'term': taxonomy.get_term('yrkesroll', yrkerf.get('varde')),
                 'vikt': yrkerf.get('vikt')
                 }
                for yrkerf in
                message.get('yrkeserfarenheter', []) if yrkerf.get('vikt', 0) > 3
            ]
        }
        annons['meriterande'] = {
            'kompetenser': [
                {'kod': kompetens.get('varde'),
                 'term': taxonomy.get_term('kompetens', kompetens.get('varde')),
                 'vikt': kompetens.get('vikt')
                 }
                for kompetens in
                message.get('kompetenser', []) if kompetens.get('vikt', 0) < 4
            ],
            'sprak': [
                {'kod': sprak.get('varde'),
                 'term': taxonomy.get_term('sprak', sprak.get('varde')),
                 'vikt': sprak.get('vikt')
                 }
                for sprak in message.get('sprak', []) if sprak.get('vikt', 0) < 4
            ],
            'utbildningsniva': [
                {'kod': utbn.get('varde'),
                 'term': taxonomy.get_term('utbildningsniva', utbn('varde')),
                 'vikt': utbn('vikt')
                 }
                for utbn in
                [message.get('utbildningsniva', {})] if utbn and utbn.get('vikt', 0) < 4
            ],
            'utbildningsinriktning': [
                {'kod': utbi.get('varde'),
                 'term': taxonomy.get_term('utbildningsinriktning', utbi.get('varde')),
                 'vikt': utbi.get('vikt')
                 }
                for utbi in
                [message.get('utbildningsinriktning', {})] if utbi.get('vikt', 0) < 4
            ],
            'yrkeserfarenheter': [
                {'kod': yrkerf.get('varde'),
                 'term': taxonomy.get_term('yrkesroll', yrkerf.get('varde')),
                 'vikt': yrkerf.get('vikt')
                 }
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
        return {
            'kod': message_value,
            'term': taxonomy.get_term(annons_key, message_value)
        }
    return None


def _add_keywords(annons):
    keywords = set()
    for key in [
        'yrkesroll.term',
        'yrkesgrupp.term',
        'yrkesomrade.term',
        'krav.kompetenser.term',
        'krav.sprak.term',
        'meriterande.kompetenser.term',
        'meriterande.sprak.term',
        'arbetsplatsadress.postort',
        'arbetsplatsadress.kommun',
        'arbetsplatsadress.lansnamn',
        'arbetsplatsadress.land',
    ]:
        values = _get_nested_value(key, annons)
        for value in values:
            for kw in _extract_taxonomy_label(value):
                keywords.add(kw)
    annons['keywords'] = list(keywords)
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
    label = label.replace('m.fl.', '').strip()
    if '-' in label:
        return [word.lower() for word in re.split(r'/', label)]
    else:
        return [word.lower().strip() for word in re.split(r'/|, | och ', label)]
