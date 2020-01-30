# ---
# jupyter:
#   jupytext:
#     formats: py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Utility Functions

# +
#paths=[]
#parts=attr.split('.')
#for i, _ in enumerate(parts):
#    if i:
#        paths.append('.'.join(parts[:-i]))
#paths.reverse()


def _isnull(obj):
    """Check an object is null."""
    if isinstance(obj, pd.DataFrame):
        return obj.empty
    elif isinstance(obj, str) and obj.lower()=='null':
        return True
    elif obj:
        return False
    return True

def _notnull(obj):
    """Check an object is not null."""
    return not _isnull(obj)

def _checkattr(obj,attr):
    """Check an object exists and is set to a non-null value."""
    
    #TO DO  - support attributes done a path, checking each step in turn
    
    if hasattr(obj,attr):
        objattr = getattr(obj, attr)
        return _notnull(objattr)
        
    return False


# + tags=["active-ipynb"]
# assert _isnull('')
# assert _isnull(None)
# assert _isnull({})
# assert _isnull([])
# assert _isnull(pd.DataFrame())
# -

# TO DO - this should go into a general utils package
def _jsInt(val):
    """Ensure we have a JSON serialisable value for an int.
       The defends against non-JSON-serialisable np.int64."""
    try:
        val = int(val)
    except:
        val = None
        
    return val


def listify(item):
    return item if isinstance(item,list) else [item] 

# + tags=["active-ipynb"]
# assert listify(1)==[1]
# assert listify('1')==['1']
# assert listify([1,2])==[1, 2]
# assert listify({'a':1})==[{'a': 1}]
