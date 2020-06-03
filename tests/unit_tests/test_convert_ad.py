from importers.platsannons.converter import convert_ad


def test_convert_ad():
    in_data = {'id': '22884504', 'epost': 'info@visitabisko.com',
               'sprak': [{'namn': 'Engelska', 'vikt': 10, 'varde': '283'},
                         {'namn': 'Svenska', 'vikt': 5, 'varde': '502'}],
               'lonTyp': {'namn': 'Fast månads- vecko- eller timlön', 'varde': '1'},
               'korkort': [{'namn': 'D', 'varde': '5'}], 'version': 1, 'annonsId': '22884504',
               'kallaTyp': 'VIA_AF_FORMULAR', 'referens': 'Trafikledare', 'expiresAt': '2019-04-15 23:59:59',
               'updatedAt': 1539958052330, 'yrkesroll': {'namn': 'Trafikledare, vägtrafik', 'varde': '5003'},
               'annonstext': 'Vi söker framtidens proffsiga pedagoger till Norlandia förskolor Lilla AlsikeKrav för tjänsten:\n\n* Erfarenhet som handledare/chef eller liknande roll samt behörig träning inom området.\n* Väl informerad kring lagar och regler inom yrket.\n* Förmågan och viljan att lära sig nya program, egenskaper och processer.\n* Exemplarisk kommunikationsförmåga i både Engelska och Svenska.\n* Mycket organiserad och en bred lednings- och samverkansförmåga.\n* Innehar alla nödvändiga körkort (minst B, D samt YKB-licens).\n* Lätta att bemöta gäster/kollegor med ett positivt och glatt beteende.\n* Bekväm att köra på vinterväglag i mörker med mycket snö och is.\nVi söker en trafikledare inför kommande höst/vintersäsong för att ansvara för bussarna, chaufförerna, logistik och hjälpa till med schemaläggning under året. Du kommer snabbt lära känna dina medarbetare och kollegor som alla jobbar för att alla ska trivas på jobbet. Ett härligt gäng med erfarenheter och egenskaper ifrån många delar av världen. Trivs du med att vara ute i naturen, möta nytt folk och arbeta både i grupp enskilt så är du den vi söker!\n\nHur ser din dag ut?\n\nDå du kommer vara ansvarig för den dagliga transporten och alla chaufförer kommer det vara mycket arbete med att hålla allting i rullning och se till så alla chaufförer, bussar är i bästa möjliga skicka och att vi levererar en professionell service till våra kunder. Din roll kommer vara uppdela i kontorsarbete, körningar, fordonshantering, basisk fordons behandling och vara huvudkontakt för alla chaufförer på plats. Du kommer även arbete nära tillsammans med resten av Visit Abiskos team för att säkerställa planeringen av schemaläggningen blir så effektiv som möjligt.\n\nDu kommer rapportera till teamet du arbetar tillsammans med dagligen, för att säkerställa att arbetet för planeringen och leveransen av alla produkter som Visit Abisko erbjuder håller den högsta möjligt standarden. Du kommer ansvara så att hela flottan med fordon underhålls till den professionella nivån som krävs inom renlighet, säkerhet och mekanisk funktionalitet.\n\nPersonliga egenskaper och utbildning:\nDu bör vara en flexibel person med lång erfarenhet inom branschen. Du möter stressiga situationer och behöver kunna ta snabba beslut för att lösa problem. Du är utåtriktad, tycker om människor och ser en utmaning i att alltid ha nöjda kunder och kollegor. För att klara jobbet krävs det att du behärskar engelska väl i tal eftersom majoriteten av den turism som sker i området under denna period är engelsktalande turister. Vi vill gärna att chaufförerna ser dig som en förebild och mentor så du behöver därför vara självsäker och inte vara rädd för att dela ut arbetsuppgifter, ge feedback osv.\nVisit Abisko är ett etablerat transport och aktivitetsföretag som har sin verksamhet i Abisko, norr om Kiruna. Vi kör persontransporter mellan Kiruna flygplats och Abisko, samt lokala bussturer i Abisko/Björkliden. Vi erbjuder även guidade turer till Narvik, och Icehotel i Jukkasjärvi m.m under vintersäsong. Vårt team består av 6 chaufförer, 2 administrativa arbetare samt en trafikledare (nytt för kommande år) och 3 ägare. Vi hyr ut enklare personalbostäder under arbetstiden och säsongsanställer personal under norrskenssäsongen (okt-mar) och skidsäsongen (apr-maj).\n\nLäs gärna mer om våra transfers & aktiviteter på vår hemsida:',
               'annonstextFormaterad': '<p><strong><em>Vi söker framtidens proffsiga pedagoger till </em></strong></p><p><strong><em>Norlandia förskolor Lilla Alsike</em></strong></p>Krav för tjänsten:\n\n* Erfarenhet som handledare/chef eller liknande roll samt behörig träning inom området.\n* Väl informerad kring lagar och regler inom yrket.\n* Förmågan och viljan att lära sig nya program, egenskaper och processer.\n* Exemplarisk kommunikationsförmåga i både Engelska och Svenska.\n* Mycket organiserad och en bred lednings- och samverkansförmåga.\n* Innehar alla nödvändiga körkort (minst B, D samt YKB-licens).\n* Lätta att bemöta gäster/kollegor med ett positivt och glatt beteende.\n* Bekväm att köra på vinterväglag i mörker med mycket snö och is.\nVi söker en trafikledare inför kommande höst/vintersäsong för att ansvara för bussarna, chaufförerna, logistik och hjälpa till med schemaläggning under året. Du kommer snabbt lära känna dina medarbetare och kollegor som alla jobbar för att alla ska trivas på jobbet. Ett härligt gäng med erfarenheter och egenskaper ifrån många delar av världen. Trivs du med att vara ute i naturen, möta nytt folk och arbeta både i grupp enskilt så är du den vi söker!\n\nHur ser din dag ut?\n\nDå du kommer vara ansvarig för den dagliga transporten och alla chaufförer kommer det vara mycket arbete med att hålla allting i rullning och se till så alla chaufförer, bussar är i bästa möjliga skicka och att vi levererar en professionell service till våra kunder. Din roll kommer vara uppdela i kontorsarbete, körningar, fordonshantering, basisk fordons behandling och vara huvudkontakt för alla chaufförer på plats. Du kommer även arbete nära tillsammans med resten av Visit Abiskos team för att säkerställa planeringen av schemaläggningen blir så effektiv som möjligt.\n\nDu kommer rapportera till teamet du arbetar tillsammans med dagligen, för att säkerställa att arbetet för planeringen och leveransen av alla produkter som Visit Abisko erbjuder håller den högsta möjligt standarden. Du kommer ansvara så att hela flottan med fordon underhålls till den professionella nivån som krävs inom renlighet, säkerhet och mekanisk funktionalitet.\n\nPersonliga egenskaper och utbildning:\nDu bör vara en flexibel person med lång erfarenhet inom branschen. Du möter stressiga situationer och behöver kunna ta snabba beslut för att lösa problem. Du är utåtriktad, tycker om människor och ser en utmaning i att alltid ha nöjda kunder och kollegor. För att klara jobbet krävs det att du behärskar engelska väl i tal eftersom majoriteten av den turism som sker i området under denna period är engelsktalande turister. Vi vill gärna att chaufförerna ser dig som en förebild och mentor så du behöver därför vara självsäker och inte vara rädd för att dela ut arbetsuppgifter, ge feedback osv.\nVisit Abisko är ett etablerat transport och aktivitetsföretag som har sin verksamhet i Abisko, norr om Kiruna. Vi kör persontransporter mellan Kiruna flygplats och Abisko, samt lokala bussturer i Abisko/Björkliden. Vi erbjuder även guidade turer till Narvik, och Icehotel i Jukkasjärvi m.m under vintersäsong. Vårt team består av 6 chaufförer, 2 administrativa arbetare samt en trafikledare (nytt för kommande år) och 3 ägare. Vi hyr ut enklare personalbostäder under arbetstiden och säsongsanställer personal under norrskenssäsongen (okt-mar) och skidsäsongen (apr-maj).\n\nLäs gärna mer om våra transfers & aktiviteter på vår hemsida:',
               'postadress': {'land': 'SE', 'postnr': '98132', 'postort': 'Kiruna', 'gatuadress': 'Steinholtzgatan 35'},
               'webbadress': 'http://www.visitabisko.com',
               'annonsrubrik': 'Trafikledare sökes till växande transportföretag i Abisko/Kiruna området',
               'antalPlatser': 1, 'arbetstidTyp': {'namn': 'Heltid', 'varde': '1'}, 'avpublicerad': False,
               'besoksadress': {'land': 'SE', 'postnr': '98132', 'postort': 'Kiruna',
                                'gatuadress': 'Steinholtzgatan 35'}, 'arbetsplatsId': '85444530',
               'telefonnummer': '0701234567', 'anstallningTyp': {'namn': 'Vanlig anställning', 'varde': '1'},
               'arbetsgivareId': '20984857', 'varaktighetTyp': {'namn': 'Tillsvidare', 'varde': '1'},
               'arbetsplatsNamn': 'Gateau AB', 'kontaktpersoner': [
            {'epost': 'fornamn.efternamn@emailadress.se', 'fornamn': 'Förnamn', 'efternamn': 'Efternamn',
             'befattning': None, 'beskrivning': None, 'mobilnummer': None, 'telefonnummer': None,
             'fackligRepresentant': False}], 'lonebeskrivning': 'Enligt överenskommelse',
               'utbildningsniva': {'namn': 'Gymnasial utbildning', 'vikt': 10, 'varde': '2'},
               'arbetsgivareNamn': 'Fazer Food Services AB',
               'arbetsplatsadress': {'lan': {'namn': 'Norrbottens län', 'varde': '25'}, 'land': None,
                                     'kommun': {'namn': 'Kiruna', 'varde': '2584'}, 'postnr': '98132',
                                     'latitud': '67.8576127184316', 'postort': 'KIRUNA', 'longitud': '20.2274210124801',
                                     'gatuadress': 'Steinholtzgatan 35', 'koordinatPrecision': None},
               'publiceringsdatum': '2018-10-19 00:00:00', 'yrkeserfarenheter': [
            {'namn': 'Bussförare/Busschaufför', 'vikt': 10, 'varde': '5561',
             'erfarenhetsniva': {'namn': '2-4 års erfarenhet', 'varde': '4'}},
            {'namn': 'Trafikledare, vägtrafik', 'vikt': 5, 'varde': '5003',
             'erfarenhetsniva': {'namn': '2-4 års erfarenhet', 'varde': '4'}}],
               'ansokningssattEpost': 'fornamn.efternamn@emailadress.se', 'ansokningssattViaAF': False,
               'avpubliceringsdatum': '2018-10-01 14:55:33', 'organisationsnummer': '5569616161',
               'tillgangTillEgenBil': False, 'villkorsbeskrivning': 'Heltidsanställning.\n1 st tjänst.',
               'ingenErfarenhetKravs': False, 'sistaAnsokningsdatum': '2019-04-15 23:59:59',
               'sistaPubliceringsdatum': '2019-04-15 23:59:59',
               'informationAnsokningssatt': 'Vi tar emot ansökan via e-post och följer upp med telefonintervjuer under ansökningsperioden.\nSkicka ditt CV och personliga brev till oss.',
               'timestamp': 1539958052330}
    out_data = convert_ad(in_data)

    print(out_data)
    assert len(out_data) == 34
    assert out_data['id'] == in_data['id']
    assert out_data['headline'] == in_data['annonsrubrik']
    assert out_data['application_deadline'] != in_data['sistaAnsokningsdatum']  # timestamp format converted
    assert out_data['application_deadline'] == '2019-04-15T23:59:59'

    assert out_data['employment_type']['concept_id'] == 'PFZr_Syz_cUq'
    assert out_data['employment_type']['label'] == 'Vanlig anställning'
    assert out_data['experience_required'] != in_data['ingenErfarenhetKravs']  # reverse
    assert out_data['timestamp'] == in_data['timestamp']
    assert out_data['driving_license'][0]['label'] == in_data['korkort'][0]['namn']
    assert out_data['driving_license'][0]['concept_id'] == 'hK1a_wsQ_4UG'
