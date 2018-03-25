def floatround(text, *params):
    return "{0:.2f}".format(float(text))

def inc(text, *params):
    return str(int(text)+1)

def dec(text, *params):
    return str(int(text)-1)

def branch(text, *params):
    if text==params[0].strip(): 
        ans = params[1].strip()
    else:
        ans = params[2].strip()
    return '[condition:#{}#]'.format(ans)

def whileNot0(text, *params):
    if int(text) > 0:
        return '[i:{i}]'.format(i=str(int(text)-1))
    return '[do:#{}#]'.format(params[0].strip())

pytracery_logic = {
    'inc': inc,
    'dec': dec,
    'branch':branch,
    'whileNot0': whileNot0, 
    
    'round':floatround,
}
