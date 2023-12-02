from jproperties import Properties

props = Properties()

def init(lang):
    if(lang and lang == 'en'):
        filename = 'i18n.properties'
    else:
        filename = 'i18n_{lang}.properties'.format(lang = lang)
    # with open('.\\texts\\'+filename, "rb") as file:
    with open('./texts/'+filename, "rb") as file:        
        props.load(file)

def get(key):
    # print(key)
    return props[key].data