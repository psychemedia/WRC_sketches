# !/Users/ajh59/anaconda3/bin/pip install git+https://github.com/pwdyson/inflect.py

#----
#inflect.py modifiers
#Mappings based on https://github.com/pwdyson/inflect.py

from pandas import isnull, to_datetime
import time

import inflect
p = inflect.engine()


def ordinal(text, *params):
    return p.ordinal(text)

def number_to_words(text, *params):
    kwargs=dict(param.split('=') for param in params)
    try:
        resp = p.number_to_words(text, **kwargs)
    except:
        resp = "no recorded"
    return resp
    

def plural(text, *params):
    if params: return p.plural(text, params[0])
    return p.plural(text)

def plural_noun(text, *params):
    if params: return p.plural_noun(text, params[0])
    return p.plural_noun(text)

def plural_verb(text, *params):
    if params: return p.plural_verb(text, params[0])
    return p.plural_verb(text)

def plural_adj(text, *params):
    if len(params): return p.plural_adj(text, params[0])
    return p.plural_adj(text)
    
def singular_noun(text, *params):
    return p.singular_noun(text)
    
def no(text, *params):
    return p.no(text)
    
def num(text, *params):
    return p.num(text)
    
def compare(text, *params):
    return p.compare(text, params[0])
    
def compare_nouns(text, *params):
    return p.compare_nouns(text,params[0])

def compare_adjs(text, *params):
    return p.compare_adjs(text,params[0])

def a(text, *params):
    return p.a(text)

def an(text, *params):
    return p.an(text)

def present_participle(text, *params):
    return p.present_participle(text)

def classical(text, *params):
    kwargs=dict(param.split('=') for param in params)
    return p.classical(text, **kwargs)
    
def gender(text, *params):
    return p.gender(text)
    
def defnoun(text, *params):
    return p.defnoun(text)
    
def defverb(text, *params):
    return p.defverb(text)
    
def defadj(text, *params):
    return p.defadj(text)
    
def defa(text, *params):
    return p.defa(text)

def defan(text, *params):
    return p.defan(text)
    
def istrue(flag, *params):
    if str(flag)=='True': return params[0]
    else: return ''
    
def isfalse(flag, *params):
    if str(flag)=='False': return params[0]
    else: return ''
    
def isNotNull(text, *params):
    if isnull(text) or text=='nan': return ''
    kwargs=dict(param.split('=') for param in params)
    pre = kwargs['pre'] if 'pre' in kwargs else ''
    post = kwargs['post'] if 'post' in kwargs else ''
    brackets = True if 'brackets' in kwargs else False
    txt = "({pre}{text}{post})" if brackets else "{pre}{text}{post}"
    return txt.format(text=text,pre=pre,post=post)
    
def ifelse(flag, *params):
    if str(flag)=='True': return params[0]
    else: return params[1]

def int_to_words(text, *params):
    try:
        text=int(float(text))
    except: pass
    kwargs=dict(param.split('=') for param in params)
    return p.number_to_words(text, **kwargs)

def pdtime(text, *params):
    if params:
        return to_datetime(text).strftime(','.join(params))
    return to_datetime(text).strftime("%b %d %Y %H:%M:%S")

def brackets(text, *params):
    if text:
        return '({})'.format(text)
    else: return ''
    

#--

inflect_english = {
    'compare':compare,
    'compare_nouns':compare_nouns,
    'compare_adjs':compare_adjs,
    'a':a,
    'a2':a,
    'an':an,
    'ordinal': ordinal,
    'number_to_words': number_to_words, 
    'plural':plural,
    'plural_noun':plural_noun,
    'plural_verb':plural_verb,
    'plural_adj':plural_adj,
    'singular_noun':singular_noun,
    'no':no,
    'num':num,
    'present_participle':present_participle,
    'classical':classical,
    'gender':gender,
    'defnoun':defnoun,
    'defverb':defverb,
    'defadj':defadj,
    'defa':defa,
    'defan':defan,
    #
    'int_to_words': int_to_words, 
    'istrue': istrue,
    'isfalse': isfalse,
    'ifelse': ifelse,
    'isNotNull':isNotNull,
    'pdtime': pdtime,
    'brackets':brackets,
}

