def floatround(text, *params):
    return "{0:.2f}".format(float(text))

def inc(text, *params):
    return str(int(float((text)+1)))

def dec(text, *params):
    return str(int(float(text)-1))

def branch(text, *params):
    if text==params[0].strip(): 
        ans = params[1].strip()
    else:
        ans = params[2].strip()
    return '[condition:#{}#]'.format(ans)

def switch(text, *params):
    return '[condition:#{}#]'.format(text)

def whileNot0(text, *params):
    if int(text) > 0:
        return '[i:{i}]'.format(i=str(int(text)-1))
    return '[do:#{}#]'.format(params[0].strip())

def takeFrom(text, *params):
    return str(int(params[0].strip()) - int(text))

def add(text, *params):
    return str(int(text) + int(params[0].strip()))

def subtract(text, *params):
    return str(int(text) - int(params[0].strip()))

def multiply(text, *params):
    return str(int(text) * int(params[0].strip()))

def divide(text, *params):
    return str(int(text) / int(params[0].strip()))
    
def neg(text, *params):
    return str(-int(text))

def gte(text, *params):
    if float(text)<=float(params[0]):
        return '[condition:#{}#]'.format(params[1])
    elif len(params)==3:
        return '[condition:#{}#]'.format(params[2])
    return '[condition:#null#]'

def eqint(text, *params):
    if float(text)==float(params[0]):
        return '[condition:#{}#]'.format(params[1])
    elif len(params)==3:
        return '[condition:#{}#]'.format(params[2])
    return '[condition:#null#]'

def prefix(text, *params):
    return '{}{}'.format( params[0], text)

def suffix(text, *params):
    return '{}{}'.format(text, params[0])

pytracery_logic = {
    'round':floatround,
    '_inc': inc,
    '_dec': dec,
    '_branch':branch,
    '_whileNot0': whileNot0,
    '_takeFrom':takeFrom,
    '_neg':neg, 
    '_add':add,
    '_subtract':subtract,
    '_multiply':multiply,
    '_divide':divide,
    '_switch':switch,
    '_gte':gte,
    '_eqint':eqint,
    '_prefix': prefix,
    '_suffix':suffix
}

'''
rules = {'origin': '[i:1]Start with: #i# #add1#, #subtract2#, #multiply3#, #divide4#',
         'add1':'[i:#i._add(10)#]#i#\n',
         'subtract2':'[i:#i._subtract(2)#]#i#\n',
         'multiply3':'[i:#i._multiply(3)#]#i#\n',
         'divide4':'[i:#i._divide(4)#]#i#\n'
        }
 
grammar = tracery.Grammar(rules)
grammar.add_modifiers(pytracery_logic)

print(grammar.flatten("#origin#"))
'''

'''
rules = {'origin': 'Loop test: \n#loop#',
         'loop':'[while:#i._whileNot0(end)#]#while##do#',
         'do':'#action##loop#',
         'action':'[j:#j._inc#]- for the #j.number_to_words.ordinal# time of asking...\n',
         'end':'All done'
        }
 
grammar = tracery.Grammar(rules)
grammar.add_modifiers(pytracery_logic)
grammar.add_modifiers(inflect_english)

print(grammar.flatten("[j:-3][i:#j._neg#][j:0]#origin#"))
'''
