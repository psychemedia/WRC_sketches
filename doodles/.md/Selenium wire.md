---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.3.1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

```python
urls = []
for request in driver.requests:
    urls.append(request.path)
urls
```

```python
#%%javascript
#if (typeof x !== 'undefined') {alert('as')}
```

```python
from seleniumwire import webdriver  # Import from seleniumwire

# Create a new instance of the Firefox driver
driver = webdriver.Firefox()
```

```python
driver.get('https://www.wrc.com/en/wrc/livetiming/page/4175----.html')

```

```python
z = None
for r in driver.requests:
    if r.path=='https://www.wrc.com/service/sasCacheApi.php?route=events%2F88%2Fstages%2F1379%2Fstagetimes%3FrallyId%3D104':
        z=r
        
r.response.body
```

```python
driver.last_request
```

```python
jr.headers
```

```python
#Clear requests...
del driver.requests
```

```python
driver.close()
```

```python
#TinyDB is a simple filebased document database 
#!pip3 install tinydb
from tinydb import TinyDB, Query
db = TinyDB('tinydb_datastore.json')
h = jr.headers
db.insert({'body':jr.body,'headers':dict(h), 'method':jr.method,
           'path':jr.path, 
           'response': {'body':jr.response.body, 'headers':dict(jr.response.headers)},
          })
```

```python
db.all()[-1]
```

```python
dir(jr)
```

```python
jr.headers
```

```python
import json
json.loads(jr.response.body.decode("utf-8"))
```

```python
! ls /Users/tonyhirst/Downloads/learn2.open.ac.uk.har
```

```python
with open('/Users/tonyhirst/Downloads/learn2.open.ac.uk.har') as f:
    ouj = json.loads(f.read())
```

```python
#https://gist.github.com/tomatohater/8853161
"""Reads a har file from the filesystem, converts to CSV, then dumps to
stdout.
"""
import argparse
import json
try:
    from urllib.parse import urlparse
except:
    from urlparse import urlparse

def harparse(harfile_path):
    """Reads a har file from the filesystem, converts to CSV, then dumps to
    stdout.
    """
    harfile = open(harfile_path)
    harfile_json = json.loads(harfile.read())
    i = 0

    for entry in harfile_json['log']['entries']:
        i = i + 1
        url = entry['request']['url']
        urlparts = urlparse(entry['request']['url'])
        size_bytes = entry['response']['bodySize']
        size_kilobytes = float(entry['response']['bodySize'])/1024
        mimetype = 'unknown'
        if 'mimeType' in entry['response']['content']:
            mimetype = entry['response']['content']['mimeType']

        print( '%s,"%s",%s,%s,%s,%s' % (i, url, urlparts.hostname, size_bytes,
                                       size_kilobytes, mimetype))

```

```python
harparse('/Users/tonyhirst/Downloads/learn2.open.ac.uk.har')
```

```python
!ls /Users/tonyhirst/Downloads/*.har
```

```python
with open('/Users/tonyhirst/Downloads/www.open.ac.uk.har') as f:
    ou = json.loads(f.read())
    
```

```python

```

```python
ou['log']['entries'][0]#['startedDateTime']
```

```python
for k in ou['log']['entries'][0].keys():
    print(k, ou['log']['entries'][0].keys())
```

## HAR Archive

```
(u'serverIPAddress', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'pageref', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'startedDateTime', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'_initiator', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'cache', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'request', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'connection', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'timings', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'_priority', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'time', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'_resourceType', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
(u'response', [u'serverIPAddress', u'pageref', u'startedDateTime', u'_initiator', u'cache', u'request', u'connection', u'timings', u'_priority', u'time', u'_resourceType', u'response'])
```

```python
predicates = [{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":".*"
    },{
      "function":"_eq",
      "arg0":["macro",1],
      "arg1":"gtm.load"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/time-planner"
    },{
      "function":"_eq",
      "arg0":["macro",1],
      "arg1":"gtm.js"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/request\/prospectus-thank-you"
    },{
      "function":"_sw",
      "arg0":["macro",0],
      "arg1":"http:\/\/www.open.ac.uk\/courses\/choose\/get-started-thank-you"
    },{
      "function":"_eq",
      "arg0":["macro",1],
      "arg1":"gtm.dom"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/request\/callback-thank-you"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/account\/createaccount-thank-you"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"openlearn"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"css2.open.ac.uk\/outis\/interest\/confirmation.aspx"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/contact\/thank-you"
    },{
      "function":"_sw",
      "arg0":["macro",0],
      "arg1":"https:\/\/www.open.ac.uk\/request\/thank-you-pages\/0006-enquire-thank-you.aspx"
    },{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":"^http(s)?:\\\/\\\/(www\\.)?open.ac.uk\\\/?(index.html)?($|\\?)",
      "ignore_case":True
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/home.html"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/life-changing-learning"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcbrand"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcgeneric"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppclcl"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcarts"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/confidence"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/open-programme"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/knowledge-pursuit"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/progress"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/kickstart"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/pg-generic"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/pg-mba"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/mie"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/registration-check"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/request\/prospectus"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses"
    },{
      "function":"_eq",
      "arg0":["macro",10],
      "arg1":"y"
    },{
      "function":"_eq",
      "arg0":["macro",3],
      "arg1":"QR1-QUAL"
    },{
      "function":"_eq",
      "arg0":["macro",11],
      "arg1":"QR1-START"
    },{
      "function":"_eq",
      "arg0":["macro",12],
      "arg1":"QR6-NAT"
    },{
      "function":"_eq",
      "arg0":["macro",13],
      "arg1":"QR6-MOD"
    },{
      "function":"_eq",
      "arg0":["macro",14],
      "arg1":"QR7-MOD"
    },{
      "function":"_eq",
      "arg0":["macro",15],
      "arg1":"QR7-NAT"
    },{
      "function":"_eq",
      "arg0":["macro",16],
      "arg1":"MISSING"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/postgraduate\/"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/find\/arts-and-humanities"
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"\/courses\/qualifications\/q(03|85|39|86)",
      "ignore_case":True
    },{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":"ttp(s)?:\/\/(www\\.)?open.ac.uk\/courses\\\/?($|\\?)",
      "ignore_case":True
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"courses\/qualifications\/[a-z]+[0-9]*(\/)?$",
      "ignore_case":True
    },{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":"ttp(s)?:\/\/(www\\.)?open.ac.uk\/postgraduate\\\/?($|\\?)",
      "ignore_case":True
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/postgraduate\/atoz"
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"postgraduate\/qualifications\/[a-z]+[0-9]*(\/)?$",
      "ignore_case":True
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/apply"
    },{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":"ttp(s)?:\/\/(www\\.)?open.ac.uk\/careers\\\/?($|\\?)",
      "ignore_case":True
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/your-course"
    },{
      "function":"_eq",
      "arg0":["macro",18],
      "arg1":"y"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"https:\/\/msds.open.ac.uk\/signon\/SAMSDefault\/SAMS001_Default.aspx"
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"\/courses\/careers.*",
      "ignore_case":True
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"\/courses\/what-study-like.*",
      "ignore_case":True
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"\/courses\/do-it.*",
      "ignore_case":True
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"\/courses\/fees-and-funding.*",
      "ignore_case":True
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"\/courses\/apply.*",
      "ignore_case":True
    },{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":"http(s)?:\/\/(www\\.)?open.ac.uk\/postgraduate\/fees-and-funding\\\/?($|\\?)",
      "ignore_case":True
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www2.open.ac.uk\/students\/getting-started"
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"\/courses\/modules\/.*",
      "ignore_case":True
    },{
      "function":"_cn",
      "arg0":["macro",17],
      "arg1":"https:\/\/www.open.ac.uk\/request\/marketing-forms\/logo-only-header\/0014-qualification-registration?\u0026qual="
    },{
      "function":"_re",
      "arg0":["macro",19],
      "arg1":"open.ac.uk\/(courses|postgraduate|contact|request)"
    },{
      "function":"_eq",
      "arg0":["macro",18],
      "arg1":"StudentHome"
    },{
      "function":"_eq",
      "arg0":["macro",18],
      "arg1":"Proposition"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/sciencejobs"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/second"
    },{
      "function":"_eq",
      "arg0":["macro",21],
      "arg1":"https:\/\/help.open.ac.uk\/students\/_data\/documents\/careers\/restricted\/career-planning-and-job-seeking-workbook.pdf?hc-career-workbook-banner-click"
    },{
      "function":"_eq",
      "arg0":["macro",1],
      "arg1":"gtm.linkClick"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_182($|,)))"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppceng"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppchealth"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcaccess"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcsci"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcsocsci"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"https:\/\/msds.open.ac.uk\/signon"
    },{
      "function":"_sw",
      "arg0":["macro",19],
      "arg1":"http:\/\/www2.open.ac.uk\/students\/help\/register-now?banner-prompt"
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"Register now"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_206($|,)))"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/business\/contact"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/business\/apprenticeships"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"http:\/\/info1.open.ac.uk\/apprenticeships-guide-2017-thank-you"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/be-inspired"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/open-qualifications"
    },{
      "function":"_eq",
      "arg0":["macro",0],
      "arg1":"http:\/\/www.open.ac.uk\/courses\/choose\/other-way"
    },{
      "function":"_eq",
      "arg0":["macro",21],
      "arg1":"http:\/\/podcast.open.ac.uk\/feeds\/3905\/20170831T143834_StudentHome-walkthrough.m4v?hc-get-ready-sh-video-click"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_226($|,)))"
    },{
      "function":"_cn",
      "arg0":["macro",21],
      "arg1":"https:\/\/www2.open.ac.uk\/students\/_data\/documents\/careers\/restricted\/career-planning-and-job-seeking-workbook.pdf#page="
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_230($|,)))"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/request\/enquiry\/course-enquiry"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/request\/callback"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"https:\/\/www.open.ac.uk\/account\/createaccount"
    },{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":"http(s)?:\/\/(www\\.)?open.ac.uk\/contact\/new\\\/?($|\\?)",
      "ignore_case":True
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"Prospects: job vacancies"
    },{
      "function":"_cn",
      "arg0":["macro",19],
      "arg1":"https:\/\/help.open.ac.uk\/job-options-available-to-graduates"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_242($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"TARGETjobs: job vacancies"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_243($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"gradireland"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_244($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"Milkround"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_245($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"Second Jobber"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_246($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"Become an apprentice"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_248($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"Prospects Degree apprenticeships"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_249($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"Graduate Recruitment Bureau"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_247($|,)))"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_250($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"Prospects: Employer profiles"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_251($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",24],
      "arg1":"TARGETjobs: Employer Insights"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_252($|,)))"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"AMC"
    },{
      "function":"_cn",
      "arg0":["macro",4],
      "arg1":"|"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"AHU"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"CYE"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"CIC"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"ENT"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"SHW"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"HWS"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"SWPE"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"SWPW"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"TML"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"LAW"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"MST"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"PSY"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"SCI"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"SSC"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"BSP"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"OPE"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"CDD"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"PGB"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"EDI"
    },{
      "function":"_sw",
      "arg0":["macro",4],
      "arg1":"MBA"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/develop"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/dream"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/mathsjobs"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppccomputing"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcedis"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcedu"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppclang"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppclaw"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcmath"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcopen"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcoubs"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/ppcpsy"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/choose\/technologyjobs"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/postgraduate\/qualifications\/f61"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/courses\/find\/engineering"
    },{
      "function":"_cn",
      "arg0":["macro",25],
      "arg1":"WA|GB"
    },{
      "function":"_eq",
      "arg0":["macro",26],
      "arg1":"topic active"
    },{
      "function":"_eq",
      "arg0":["macro",26],
      "arg1":"category"
    },{
      "function":"_eq",
      "arg0":["macro",26],
      "arg1":"article"
    },{
      "function":"_eq",
      "arg0":["macro",19],
      "arg1":"help.open.ac.uk\/"
    },{
      "function":"_eq",
      "arg0":["macro",1],
      "arg1":"gtm.click"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"https:\/\/help.open.ac.uk\/countdown-to-study\/2018\/october"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"https:\/\/help.open.ac.uk\/countdown-to-study\/2019\/october"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_290($|,)))"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"https:\/\/help.open.ac.uk\/exam-quick-answers"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_294($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",21],
      "arg1":"https:\/\/msds.open.ac.uk\/module-chooser\/index.aspx"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_304($|,)))"
    },{
      "function":"_re",
      "arg0":["macro",19],
      "arg1":"stem\\.open\\.ac.uk.*",
      "ignore_case":True
    },{
      "function":"_eq",
      "arg0":["macro",0],
      "arg1":"http:\/\/www2.open.ac.uk\/students\/communications\/world-mental-health-day"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_319($|,)))"
    },{
      "function":"_eq",
      "arg0":["macro",0],
      "arg1":"http:\/\/www2.open.ac.uk\/students\/communications\/world-mental-health-day-201810"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_320($|,)))"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"https:\/\/help.open.ac.uk\/how-do-i-become-a"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_321($|,)))"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"https:\/\/help.open.ac.uk\/new-to-ou-study"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_326($|,)))"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"https:\/\/help.open.ac.uk\/discover-your-subject"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_327($|,)))"
    },{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":"http(s)?:\/\/(www\\.)?open.ac.uk\/contact\\\/?($|\\?)",
      "ignore_case":True
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"css2.open.ac.uk\/outis\/1bd\/o1bdHomePage.asp"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"css2.open.ac.uk\/Outis\/1bd\/O1BDCTP.asp"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/request\/marketing-forms\/logo-only-header\/0059-qualification-registration-v5-uk.aspx"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/request\/thank-you-pages\/0059-qualification-registration-thank-you.aspx"
    },{
      "function":"_cn",
      "arg0":["macro",24],
      "arg1":"Go to Summer Careers Surgery forum"
    },{
      "function":"_cn",
      "arg0":["macro",19],
      "arg1":"https:\/\/help.open.ac.uk\/careers-consultation"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_362($|,)))"
    },{
      "function":"_cn",
      "arg0":["macro",21],
      "arg1":".pdf"
    },{
      "function":"_re",
      "arg0":["macro",22],
      "arg1":"(^$|((^|,)911419_363($|,)))"
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"\/courses\/qualifications\/.*",
      "ignore_case":True
    },{
      "function":"_cn",
      "arg0":["macro",17],
      "arg1":"outis"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"GD=y"
    },{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":"http(s)?:\\\/\\\/learn2\\.open.ac.uk\\\/",
      "ignore_case":True
    },{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":"http(s)?:\\\/\\\/msds\\.open.ac.uk\/students\\\/?($|\\?)",
      "ignore_case":True
    },{
      "function":"_cn",
      "arg0":["macro",19],
      "arg1":"https:\/\/www.open.ac.uk\/request\/prospectus?CATCODE=LABHDU"
    },{
      "function":"_cn",
      "arg0":["macro",0],
      "arg1":"www.open.ac.uk\/business"
    },{
      "function":"_eq",
      "arg0":["macro",29],
      "arg1":"continue"
    },{
      "function":"_cn",
      "arg0":["macro",30],
      "arg1":"samsStaffID"
    },{
      "function":"_re",
      "arg0":["macro",0],
      "arg1":"^http(s)?:\/\/(www\\.)?open.ac.uk\/careers\\\/?($|\\?)",
      "ignore_case":True
    },{
      "function":"_re",
      "arg0":["macro",17],
      "arg1":"\/courses\/qualifications\/(r28|q67-citp|q67|q98|q43|q07|q84|r23-psyc|q82|q83|q23|x09|w42-citp|w42|w57|w63|w45|w09|t13-citp|t13|t22|t01|t24)",
      "ignore_case":True
    },{
      "function":"_cn",
      "arg0":["macro",17],
      "arg1":"\/courses\/careers\/psychology"
    }]
```

```python
import argparse
import json
import csv
import os
try:
    from urllib.parse import urlparse
except:
    from urlparse import urlparse
    
def harpaths(harfile_path):
    """Reads a har file from the filesystem, converts to CSV, and dumps to
    file.
    """
    harfile = open(harfile_path)
    harfile_json = json.loads(harfile.read())
    with open( os.path.splitext(harfile_path)[0] + '.csv', 'w') as f:
        csv_file = csv.writer(f)
        csv_file.writerow(['time', 'hostname', 'url'])
        for entry in harfile_json['log']['entries']:
            rtime = entry['startedDateTime']
            url = entry['request']['url']
            urlparts = urlparse(entry['request']['url'])
            if 'facebook' in urlparts.hostname:
                csv_file.writerow([rtime,urlparts.hostname, url ])
        
if __name__ == '__main__':
    argparser = argparse.ArgumentParser(
        prog='parsehar',
        description='Parse .har files into comma separated values (csv).')
    argparser.add_argument('harfile', type=str, nargs=1,
                        help='path to harfile to be processed.')
    args = argparser.parse_args()

    harpaths(args.harfile[0])
```

```python
from_ga_tag_manager= [{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003EsetTimeout(function(){var a=document.createElement(\"script\"),b=document.getElementsByTagName(\"script\")[0];a.src=document.location.protocol+\"\/\/script.crazyegg.com\/pages\/scripts\/0017\/2589.js?\"+Math.floor((new Date).getTime()\/36E5);a.async=!0;a.type=\"text\/javascript\";b.parentNode.insertBefore(a,b)},1);\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":9
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003Evar mcDaysToSet=365,mcExpDate=new Date;mcExpDate.setDate(mcExpDate.getDate()+mcDaysToSet);document.cookie=\"GD\\x3dy;path\\x3d\/; expires\\x3d \"+mcExpDate.toUTCString();\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":42
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\n\u003Cscript type=\"text\/gtmscript\"\u003E!function(b,e,f,g,a,c,d){b.fbq||(a=b.fbq=function(){a.callMethod?a.callMethod.apply(a,arguments):a.queue.push(arguments)},b._fbq||(b._fbq=a),a.push=a,a.loaded=!0,a.version=\"2.0\",a.queue=[],c=e.createElement(f),c.async=!0,c.src=g,d=e.getElementsByTagName(f)[0],d.parentNode.insertBefore(c,d))}(window,document,\"script\",\"https:\/\/connect.facebook.net\/en_US\/fbevents.js\");fbq(\"init\",\"870490019710405\");fbq(\"track\",\"PageView\");\u003C\/script\u003E\n\u003Cnoscript\u003E\n\u003Cimg height=\"1\" width=\"1\" src=\"https:\/\/www.facebook.com\/tr?id=870490019710405\u0026amp;ev=PageView\n\u0026amp;noscript=1\"\u003E\n\u003C\/noscript\u003E\n\n\n",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":51
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\n\u003Cscript type=\"text\/gtmscript\"\u003E!function(b,c,d,a){b.twq||(a=b.twq=function(){a.exe?a.exe.apply(a,arguments):a.queue.push(arguments)},a.version=\"1\",a.queue=[],t=c.createElement(d),t.async=!0,t.src=\"\/\/static.ads-twitter.com\/uwt.js\",s=c.getElementsByTagName(d)[0],s.parentNode.insertBefore(t,s))}(window,document,\"script\");twq(\"init\",\"nv56j\");twq(\"track\",\"PageView\");\u003C\/script\u003E\n",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":52
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003Evar mcDaysToSet=365,mcExpDate=new Date;mcExpDate.setDate(mcExpDate.getDate()+mcDaysToSet);document.cookie=\"VLE\\x3dy;path\\x3d\/;domain\\x3dopen.ac.uk; expires\\x3d \"+mcExpDate.toUTCString();\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":53
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003Evar mcDaysToSet=365,mcExpDate=new Date;mcExpDate.setDate(mcExpDate.getDate()+mcDaysToSet);document.cookie=\"VLE\\x3dStudentHome;path\\x3d\/;domain\\x3dopen.ac.uk; expires\\x3d \"+mcExpDate.toUTCString();\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":58
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003Evar mcDaysToSet=365,mcExpDate=new Date;mcExpDate.setDate(mcExpDate.getDate()+mcDaysToSet);document.cookie=\"VLE\\x3dProposition;path\\x3d\/;domain\\x3dopen.ac.uk; expires\\x3d \"+mcExpDate.toUTCString();\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":59
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\n\u003Cscript type=\"text\/gtmscript\"\u003E!function(b,e,f,g,a,c,d){b.fbq||(a=b.fbq=function(){a.callMethod?a.callMethod.apply(a,arguments):a.queue.push(arguments)},b._fbq||(b._fbq=a),a.push=a,a.loaded=!0,a.version=\"2.0\",a.queue=[],c=e.createElement(f),c.async=!0,c.src=g,d=e.getElementsByTagName(f)[0],d.parentNode.insertBefore(c,d))}(window,document,\"script\",\"https:\/\/connect.facebook.net\/en_US\/fbevents.js\");fbq(\"init\",\"1816296611934572\");fbq(\"track\",\"PageView\");\u003C\/script\u003E\n\u003Cnoscript\u003E\u003Cimg height=\"1\" width=\"1\" style=\"display:none\" src=\"https:\/\/www.facebook.com\/tr?id=1816296611934572\u0026amp;ev=PageView\u0026amp;noscript=1\"\u003E\u003C\/noscript\u003E\n\n",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":64
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\n\u003Cscript type=\"text\/gtmscript\"\u003E!function(b,e,f,g,a,c,d){b.fbq||(a=b.fbq=function(){a.callMethod?a.callMethod.apply(a,arguments):a.queue.push(arguments)},b._fbq||(b._fbq=a),a.push=a,a.loaded=!0,a.version=\"2.0\",a.queue=[],c=e.createElement(f),c.async=!0,c.src=g,d=e.getElementsByTagName(f)[0],d.parentNode.insertBefore(c,d))}(window,document,\"script\",\"https:\/\/connect.facebook.net\/en_US\/fbevents.js\");fbq(\"init\",\"870490019710405\");fbq(\"track\",\"PageView\");fbq(\"track\",\"Lead\");\u003C\/script\u003E\n\u003Cnoscript\u003E\u003Cimg height=\"1\" width=\"1\" style=\"display:none\" src=\"https:\/\/www.facebook.com\/tr?id=870490019710405\u0026amp;ev=PageView\u0026amp;noscript=1\"\u003E\u003C\/noscript\u003E\n",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":69
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\n\u003Cscript type=\"text\/gtmscript\"\u003E!function(b,c,d,a){b.twq||(a=b.twq=function(){a.exe?a.exe.apply(a,arguments):a.queue.push(arguments)},a.version=\"1\",a.queue=[],t=c.createElement(d),t.async=!0,t.src=\"\/\/static.ads-twitter.com\/uwt.js\",s=c.getElementsByTagName(d)[0],s.parentNode.insertBefore(t,s))}(window,document,\"script\");twq(\"init\",\"nv56j\");twq(\"track\",\"PageView\");\u003C\/script\u003E\n",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":70
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":" \u003Cdiv id=\"ttdUniversalPixelTag02113eba2a854414a5a12f1a483ba508\" style=\"display:none\"\u003E\n        \u003Cscript data-gtmsrc=\"https:\/\/js.adsrvr.org\/up_loader.1.1.0.js\" type=\"text\/gtmscript\"\u003E\u003C\/script\u003E\n        \u003Cscript type=\"text\/gtmscript\"\u003E(function(a){\"function\"===typeof TTDUniversalPixelApi\u0026\u0026(a=new TTDUniversalPixelApi,a.init(\"zi2lpfp\",[\"3wkwcb3\"],\"https:\/\/insight.adsrvr.org\/track\/up\",\"ttdUniversalPixelTag02113eba2a854414a5a12f1a483ba508\"))})(this);\u003C\/script\u003E\n    \u003C\/div\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":73
    },{
      "function":"__html",
      "unlimited":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003E(function(b,c,e,f,d){b[d]=b[d]||[];var g=function(){var a={ti:\"5474847\"};a.q=b[d];b[d]=new UET(a);b[d].push(\"pageLoad\")};var a=c.createElement(e);a.src=f;a.async=1;a.onload=a.onreadystatechange=function(){var b=this.readyState;b\u0026\u0026\"loaded\"!==b\u0026\u0026\"complete\"!==b||(g(),a.onload=a.onreadystatechange=null)};c=c.getElementsByTagName(e)[0];c.parentNode.insertBefore(a,c)})(window,document,\"script\",\"\/\/bat.bing.com\/bat.js\",\"uetq\");\u003C\/script\u003E\u003Cnoscript\u003E\u003Cimg src=\"\/\/bat.bing.com\/action\/0?ti=5474847\u0026amp;Ver=2\" height=\"0\" width=\"0\" style=\"display:none; visibility: hidden;\"\u003E\u003C\/noscript\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":74
    },{
      "function":"__html",
      "unlimited":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003E(function(b,c,e,f,d){b[d]=b[d]||[];var g=function(){var a={ti:\"5474847\"};a.q=b[d];b[d]=new UET(a);b[d].push(\"pageLoad\")};var a=c.createElement(e);a.src=f;a.async=1;a.onload=a.onreadystatechange=function(){var b=this.readyState;b\u0026\u0026\"loaded\"!==b\u0026\u0026\"complete\"!==b||(g(),a.onload=a.onreadystatechange=null)};c=c.getElementsByTagName(e)[0];c.parentNode.insertBefore(a,c)})(window,document,\"script\",\"\/\/bat.bing.com\/bat.js\",\"uetq\");\u003C\/script\u003E\u003Cnoscript\u003E\u003Cimg src=\"\/\/bat.bing.com\/action\/0?ti=5474847\u0026amp;Ver=2\" height=\"0\" width=\"0\" style=\"display:none; visibility: hidden;\"\u003E\u003C\/noscript\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":75
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cimg height=\"1\" width=\"1\" alt=\"\" style=\"display:none\" src=\"https:\/\/traffic.outbrain.com\/network\/trackpxl?advid=67602\u0026amp;action=view \"\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":76
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cimg height=\"1\" width=\"1\" alt=\"\" style=\"display:none\" src=\"https:\/\/traffic.outbrain.com\/network\/trackpxl?advid=67404\u0026amp;action=view \"\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":78
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cimg height=\"1\" width=\"1\" alt=\"\" style=\"display:none\" src=\"https:\/\/traffic.outbrain.com\/network\/trackpxl?advid=67601\u0026amp;action=view \"\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":79
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cimg height=\"1\" width=\"1\" alt=\"\" style=\"display:none\" src=\"https:\/\/traffic.outbrain.com\/network\/trackpxl?advid=67600\u0026amp;action=view \"\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":84
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003E$(\"h1\").first().text(\"Take your first steps with The Open University\");$(\".int-promo\").first().find(\"p:eq(0)\").text(\"We know that requesting a prospectus may seem a bit daunting.  If you are new to distance learning, we would suggest requesting our Undergraduate Courses Prospectus which is designed to start you off on this exciting journey.  If you think you already know what subject you would like to study, then jump straight on in and choose that.  You can choose up to three prospectuses so you are not limited to one!\");\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":116
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\n\u003Cscript type=\"text\/gtmscript\"\u003E!function(b,e,f,g,a,c,d){b.fbq||(a=b.fbq=function(){a.callMethod?a.callMethod.apply(a,arguments):a.queue.push(arguments)},b._fbq||(b._fbq=a),a.push=a,a.loaded=!0,a.version=\"2.0\",a.queue=[],c=e.createElement(f),c.async=!0,c.src=g,d=e.getElementsByTagName(f)[0],d.parentNode.insertBefore(c,d))}(window,document,\"script\",\"https:\/\/connect.facebook.net\/en_US\/fbevents.js\");fbq(\"init\",\"870490019710405\");fbq(\"track\",\"PageView\");fbq(\"track\",\"CompleteRegistration\");\u003C\/script\u003E\n\u003Cnoscript\u003E\u003Cimg height=\"1\" width=\"1\" style=\"display:none\" src=\"https:\/\/www.facebook.com\/tr?id=870490019710405\u0026amp;ev=PageView\u0026amp;noscript=1\"\u003E\u003C\/noscript\u003E\n",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":119
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\n\u003Cscript type=\"text\/gtmscript\"\u003E!function(b,e,f,g,a,c,d){b.fbq||(a=b.fbq=function(){a.callMethod?a.callMethod.apply(a,arguments):a.queue.push(arguments)},b._fbq||(b._fbq=a),a.push=a,a.loaded=!0,a.version=\"2.0\",a.queue=[],c=e.createElement(f),c.async=!0,c.src=g,d=e.getElementsByTagName(f)[0],d.parentNode.insertBefore(c,d))}(window,document,\"script\",\"https:\/\/connect.facebook.net\/en_US\/fbevents.js\");fbq(\"init\",\"870490019710405\");fbq(\"track\",\"PageView\");fbq(\"track\",\"Purchase\",{value:\"0.00\",currency:\"GBP\"});\u003C\/script\u003E\n\u003Cnoscript\u003E\u003Cimg height=\"1\" width=\"1\" style=\"display:none\" src=\"https:\/\/www.facebook.com\/tr?id=870490019710405\u0026amp;ev=PageView\u0026amp;noscript=1\"\u003E\u003C\/noscript\u003E\n",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":120
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003E_linkedin_data_partner_id=\"195834\";\u003C\/script\u003E\u003Cscript type=\"text\/gtmscript\"\u003E(function(){var b=document.getElementsByTagName(\"script\")[0],a=document.createElement(\"script\");a.type=\"text\/javascript\";a.async=!0;a.src=\"https:\/\/snap.licdn.com\/li.lms-analytics\/insight.min.js\";b.parentNode.insertBefore(a,b)})();\u003C\/script\u003E\n\u003Cnoscript\u003E\n\u003Cimg height=\"1\" width=\"1\" style=\"display:none;\" alt=\"\" src=\"https:\/\/dc.ads.linkedin.com\/collect\/?pid=195834\u0026amp;fmt=gif\"\u003E\n\u003C\/noscript\u003E\n",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":185
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":" ",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":188
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\" data-gtmsrc=\"https:\/\/go.affec.tv\/j\/5ad5bfabefa76f0009ef4b01\" async=\"True\"\u003E\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":192
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\n\u003Cscript type=\"text\/gtmscript\"\u003E!function(b,e,f,g,a,c,d){b.fbq||(a=b.fbq=function(){a.callMethod?a.callMethod.apply(a,arguments):a.queue.push(arguments)},b._fbq||(b._fbq=a),a.push=a,a.loaded=!0,a.version=\"2.0\",a.queue=[],c=e.createElement(f),c.async=!0,c.src=g,d=e.getElementsByTagName(f)[0],d.parentNode.insertBefore(c,d))}(window,document,\"script\",\"https:\/\/connect.facebook.net\/en_US\/fbevents.js\");fbq(\"init\",\"870490019710405\");fbq(\"track\",\"Staff\");\u003C\/script\u003E\n\u003Cnoscript\u003E\u003Cimg height=\"1\" width=\"1\" style=\"display:none\" src=\"https:\/\/www.facebook.com\/tr?id=870490019710405\u0026amp;ev=PageView\u0026amp;noscript=1\"\u003E\u003C\/noscript\u003E\n\n",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":196
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003Econsole.log(\"GTM test s-variable\\x3d \"+s);setTimeout(function(){console.log(\"timeout 5 seconds, GTM test s-variable\\x3d \"+s)},5E3);\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":198
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003Econsole.log(\"GTM test s-variable\\x3d \"+s);setTimeout(function(){console.log(\"timeout 5 seconds, GTM test s-variable\\x3d \"+s)},5E3);\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":200
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript type=\"text\/gtmscript\"\u003Econsole.log(\"GTM test s-variable\\x3d \"+s);setTimeout(function(){console.log(\"timeout 5 seconds, GTM test s-variable\\x3d \"+s)},5E3);\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":201
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\n\u003Cscript type=\"text\/gtmscript\"\u003E!function(b,e,f,g,a,c,d){b.fbq||(a=b.fbq=function(){a.callMethod?a.callMethod.apply(a,arguments):a.queue.push(arguments)},b._fbq||(b._fbq=a),a.push=a,a.loaded=!0,a.version=\"2.0\",a.queue=[],c=e.createElement(f),c.async=!0,c.src=g,d=e.getElementsByTagName(f)[0],d.parentNode.insertBefore(c,d))}(window,document,\"script\",\"https:\/\/connect.facebook.net\/en_US\/fbevents.js\");fbq(\"init\",\"696418690558071\");fbq(\"track\",\"PageView\");\u003C\/script\u003E\n\u003Cnoscript\u003E\u003Cimg height=\"1\" width=\"1\" style=\"display:none\" src=\"https:\/\/www.facebook.com\/tr?id=696418690558071\u0026amp;ev=PageView\u0026amp;noscript=1\"\u003E\u003C\/noscript\u003E\n\n",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":203
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":" \u003Cscript async data-gtmsrc=\"https:\/\/www.googletagmanager.com\/gtag\/js?id=AW-1071443088\" type=\"text\/gtmscript\"\u003E\u003C\/script\u003E \u003Cscript type=\"text\/gtmscript\"\u003Ewindow.dataLayer=window.dataLayer||[];function gtag(){dataLayer.push(arguments)}gtag(\"js\",new Date);gtag(\"config\",\"AW-1071443088\");\u003C\/script\u003E ",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":204
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cimg src=\"https:\/\/secure.adnxs.com\/px?id=1073207\u0026amp;seg=16903548\u0026amp;t=2\" width=\"1\" height=\"1\"\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":247
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cimg src=\"https:\/\/secure.adnxs.com\/px?id=1073206\u0026amp;seg=16903546\u0026amp;t=2\" width=\"1\" height=\"1\"\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":248
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cimg src=\"https:\/\/secure.adnxs.com\/px?id=1073670\u0026amp;seg=16978053\u0026amp;order_id=[ORDER_ID]\u0026amp;value=[REVENUE]\u0026amp;t=2\" width=\"1\" height=\"1\"\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":249
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cimg src=\"https:\/\/secure.adnxs.com\/px?id=1073671\u0026amp;seg=16978054\u0026amp;order_id=[ORDER_ID]\u0026amp;value=[REVENUE]\u0026amp;t=2\" width=\"1\" height=\"1\"\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":250
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cimg src=\"https:\/\/secure.adnxs.com\/px?id=1073672\u0026amp;seg=16978055\u0026amp;order_id=[ORDER_ID]\u0026amp;value=[REVENUE]\u0026amp;t=2\" width=\"1\" height=\"1\"\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":251
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cimg src=\"https:\/\/secure.adnxs.com\/px?id=1073673\u0026amp;seg=16978056\u0026amp;order_id=[ORDER_ID]\u0026amp;value=[REVENUE]\u0026amp;t=2\" width=\"1\" height=\"1\"\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":252
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript language=\"JavaScript1.1\" async data-gtmsrc=\"\/\/pixel.mathtag.com\/event\/js?mt_id=1397094\u0026amp;mt_adid=221251\u0026amp;mt_exem=\u0026amp;mt_excl=\u0026amp;v1=\u0026amp;v2=\u0026amp;v3=\u0026amp;s1=\u0026amp;s2=\u0026amp;s3=\" type=\"text\/gtmscript\"\u003E\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":253
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript language=\"JavaScript1.1\" async data-gtmsrc=\"\/\/pixel.mathtag.com\/event\/js?mt_id=1397095\u0026amp;mt_adid=221251\u0026amp;mt_exem=\u0026amp;mt_excl=\u0026amp;v1=\u0026amp;v2=\u0026amp;v3=\u0026amp;s1=\u0026amp;s2=\u0026amp;s3=\" type=\"text\/gtmscript\"\u003E\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":254
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript language=\"JavaScript1.1\" async data-gtmsrc=\"\/\/pixel.mathtag.com\/event\/js?mt_id=1397097\u0026amp;mt_adid=221251\u0026amp;mt_exem=\u0026amp;mt_excl=\u0026amp;v1=\u0026amp;v2=\u0026amp;v3=\u0026amp;s1=\u0026amp;s2=\u0026amp;s3=\" type=\"text\/gtmscript\"\u003E\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":255
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript language=\"JavaScript1.1\" async data-gtmsrc=\"\/\/pixel.mathtag.com\/event\/js?mt_id=1397098\u0026amp;mt_adid=221251\u0026amp;mt_exem=\u0026amp;mt_excl=\u0026amp;v1=\u0026amp;v2=\u0026amp;v3=\u0026amp;s1=\u0026amp;s2=\u0026amp;s3=\" type=\"text\/gtmscript\"\u003E\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":256
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript language=\"JavaScript1.1\" async data-gtmsrc=\"\/\/pixel.mathtag.com\/event\/js?mt_id=1397101\u0026amp;mt_adid=221251\u0026amp;mt_exem=\u0026amp;mt_excl=\u0026amp;v1=\u0026amp;v2=\u0026amp;v3=\u0026amp;s1=\u0026amp;s2=\u0026amp;s3=\" type=\"text\/gtmscript\"\u003E\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":257
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript language=\"JavaScript1.1\" async data-gtmsrc=\"\/\/pixel.mathtag.com\/event\/js?mt_id=1397102\u0026amp;mt_adid=221251\u0026amp;mt_exem=\u0026amp;mt_excl=\u0026amp;v1=\u0026amp;v2=\u0026amp;v3=\u0026amp;s1=\u0026amp;s2=\u0026amp;s3=\" type=\"text\/gtmscript\"\u003E\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":258
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript language=\"JavaScript1.1\" async data-gtmsrc=\"\/\/pixel.mathtag.com\/event\/js?mt_id=1397103\u0026amp;mt_adid=221251\u0026amp;mt_exem=\u0026amp;mt_excl=\u0026amp;v1=\u0026amp;v2=\u0026amp;v3=\u0026amp;s1=\u0026amp;s2=\u0026amp;s3=\" type=\"text\/gtmscript\"\u003E\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":259
    },{
      "function":"__html",
      "once_per_event":True,
      "vtp_html":"\u003Cscript language=\"JavaScript1.1\" async data-gtmsrc=\"\/\/pixel.mathtag.com\/event\/js?mt_id=1397104\u0026amp;mt_adid=221251\u0026amp;mt_exem=\u0026amp;mt_excl=\u0026amp;v1=\u0026amp;v2=\u0026amp;v3=\u0026amp;s1=\u0026amp;s2=\u0026amp;s3=\" type=\"text\/gtmscript\"\u003E\u003C\/script\u003E",
      "vtp_supportDocumentWrite":False,
      "vtp_enableIframeMode":False,
      "vtp_enableEditJsMacroBehavior":False,
      "tag_id":260
    }]
```

```python
json.loads(from_ga_tag_manager)
```

```python
predicates[1]
```

```python
p = [31, 83, 143, 144, 4, 6, 7, 8, 11, 12 ]
for pi in p:
    print(predicates[pi+1])
```

```python
i=0
for j in predicates:
    if 'learn' in j['arg1']:
        print(i, j)
    i+=1
```

```python
!ls -al *.har
```

```python
import pandas as pd
pdj =pd.read_json('learn2.open.ac.uk.har')
```

```python
pdj.head()
```

```python
pdj.loc['entries'][0][1]['pageref']
```

```python
pdj.loc['pages'][0]
```

`har` archive has `pages` and `entries` attributes. Pages ar the top level pages that are loaded and entries describe the resources loaded within the context of that page, cross-referencing using the `pageref` attribute.

```python

```
