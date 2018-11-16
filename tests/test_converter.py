import pytest, sys, logging
from importers.platsannons import converter
from importers.repository import taxonomy

log = logging.getLogger(__name__)

@pytest.mark.integration
@pytest.mark.parametrize("annons_key", ['anstallningstyp', '', None, 'sprak'])
@pytest.mark.parametrize("message_key", ['anstallningTyp','mkey', '', None, 'sprak'] )
@pytest.mark.parametrize("message_dict", [{"anstallningTyp": {"varde": "1"}}, {"anstallningTyp": {"varde": "2"}}
                                          ,{'': {'varde': 'v2'}}, {'mkey': {'EJvarde': 'v1'}} ,{} ,None, {"sprak": {"varde": "424"}} ] )
def test_expand_taxonomy_value(annons_key, message_key, message_dict):
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    d =  converter._expand_taxonomy_value(annons_key, message_key, message_dict)
    print(d)
    if not message_dict: #message_dict is empty
        assert d is None
        return
    message_value = message_dict.get(message_key, {}).get('varde')
    if message_value :
        assert d['kod'] == message_value
        assert d['term'] == taxonomy.get_term(annons_key, message_value)
    else:
        assert d is None

@pytest.mark.integration
def test_convert_message(msg): # see msg fixture in conftest.py
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    print(msg)
    annons = converter.convert_message(msg)
    print(annons)
    if 'annons' in msg and 'version' in msg:
        print("Message is not in correct format, going to be formatted...")
        message = msg['annons']
        assert annons['id'] == message.get('annonsId')
        assert annons['rubrik'] == message.get('annonsrubrik')
        assert annons['sista_ansokningsdatum'] == converter._isodate(message.get('sistaAnsokningsdatum'))
        assert annons['antal_platser'] == message.get('antalPlatser')
        assert annons['beskrivning'] == {
            'information': message.get('ftgInfo'),
            'behov': message.get('beskrivningBehov'),
            'krav': message.get('beskrivningKrav'),
            'villkor': message.get('villkorsbeskrivning'),
            'annonstext': message.get('annonstext'),
        }
        assert annons['arbetsplats_id'] == message.get('arbetsplatsId')
        assert annons['anstallningstyp'] == converter._expand_taxonomy_value('anstallningstyp','anstallningTyp', message)
        assert annons['lonetyp'] == converter._expand_taxonomy_value('lonetyp', 'lonTyp', message)
        assert annons['varaktighet'] == converter._expand_taxonomy_value('varaktighet', 'varaktighetTyp', message)
        assert annons['arbetstidstyp'] == converter._expand_taxonomy_value('arbetstidstyp', 'arbetstidTyp', message)
        assert annons['arbetsomfattning'] == {
            'min': message.get('arbetstidOmfattningFran'),
            'max': message.get('arbetstidOmfattningTill')
        }
        assert annons['tilltrade'] == message.get('tilltrade')
        assert annons['arbetsgivare'] == {
            'id': message.get('arbetsgivareId'),
            'telefonnummer': message.get('telefonnummer'),
            'epost': message.get('epost'),
            'webbadress': message.get('webbadress'),
            'organisationsnummer': message.get('organisationsnummer'),
            'namn': message.get('arbetsgivareNamn'),
            'arbetsplats': message.get('arbetsplatsNamn')
        }
        assert annons['ansokningsdetaljer'] == {
            'information': message.get('informationAnsokningssatt'),
            'referens': message.get('referens'),
            'epost': message.get('ansokningssattEpost'),
            'via_af': message.get('ansokningssattViaAF'),
            'webbadress': message.get('ansokningssattWebbadress'),
            'annat': message.get('ansokningssattAnnatSatt')
        }
        assert annons['erfarenhet_kravs'] is not message.get('ingenErfarenhetKravs', False)
        assert annons['egen_bil'] == message.get('tillgangTillEgenBil', False)
        if message.get('korkort') :
            assert annons['korkort_kravs'] is True
        else:
            assert annons['korkort_kravs'] is False
        if 'yrkesroll' in message:
            yrkesroll = taxonomy.get_entity('yrkesroll', message.get('yrkesroll', {}).get('varde'))
            if yrkesroll and 'parent' in yrkesroll:
                yrkesgrupp = yrkesroll.pop('parent')
                yrkesomrade = yrkesgrupp.pop('parent')
                assert annons['yrkesroll'] ==  {'kod': yrkesroll['id'],  'term': yrkesroll['label']}
                assert annons['yrkesgrupp'] == {'kod': yrkesgrupp['id'], 'term': yrkesgrupp['label']}
                assert annons['yrkesomrade'] =={'kod': yrkesomrade['id'],'term': yrkesomrade['label']}

        if message.get('arbetsplatsadress'):
            arbplatsmessage = message.get('arbetsplatsadress')
            assert annons['arbetsplatsadress'] == {
                'kommunkod': arbplatsmessage.get('kommun', {}).get('varde'),
                'lan': arbplatsmessage.get('lan', {}).get('varde'),
                'gatuadress': arbplatsmessage.get('gatuadress'),
                'postnummer': arbplatsmessage.get('postnr'),
                'postort': arbplatsmessage.get('postort'),
                'coordinates': [float(arbplatsmessage.get('longitud')),
                                float(arbplatsmessage.get('latitud'))]
            }
        else:
            assert annons['arbetsplatsadress'] == {'kommun': None,
                                                   'lan': None,
                                                   'gatuadress': None,
                                                   'postnummer': None,
                                                   'postort': None,
                                                   'coordinates': None}
        assert annons['krav'] == {
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
        assert annons['meriterande'] == {
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
                [message.get('utbildningsniva', {})] if utbn and utbn.get('vikt', 0) < 4 #TODO ask Markus about "and"
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
        assert annons['publiceringsdatum'] == converter._isodate(message.get('publiceringsdatum'))
        assert annons['kalla'] == message.get('kalla')
        assert annons['publiceringskanaler'] == {
            'platsbanken': message.get('publiceringskanalPlatsbanken', False),
            'ais': message.get('publiceringskanalAis', False),
            'platsjournalen': message.get('publiceringskanalPlatsjournalen', False),
        }
        assert annons['status'] == {
            'publicerad': (message.get('status') == 'PUBLICERAD' or message.get('status') == 'GODKAND_FOR_PUBLICERING'),
            'sista_publiceringsdatum': converter._isodate(message.get('sistaPubliceringsdatum')),
            'skapad': converter._isodate(message.get('skapadTid')),
            'skapad_av': message.get('skapadAv'),
            'uppdaterad': converter._isodate(message.get('uppdateradTid')),
            'uppdaterad_av': message.get('uppdateradAv'),
            'anvandarId': message.get('anvandarId')
        }
    else:
        print("Message is already in correct format")
        assert msg == annons

@pytest.mark.unit
@pytest.mark.parametrize("parsing_date", ['180924-00:00', '20180924T', 'mon sep 24', '00:00:00' ])
@pytest.mark.parametrize("not_parsing_date", ['20180101f', '2099-13-32', '18-09-24:01:01', '', None, []])
def test_isodate(parsing_date, not_parsing_date):
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    if not not_parsing_date:
        assert converter._isodate(not_parsing_date) is None
        return
    assert converter._isodate(not_parsing_date) is None
    assert converter._isodate(parsing_date) is not None
