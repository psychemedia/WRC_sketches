---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.0'
      jupytext_version: 0.8.6
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

```python
url='https://mtc1.uk/Entry/CSRTC19/EntryList.php'
```

```python
import unicodedata

def cleanString(s):
    s = unicodedata.normalize("NFKD", str(s))
    #replace multiple whitespace with single space
    s = ' '.join(s.split())
    
    return s
    
```

```python
import requests
import pandas as pd
import lxml.html as LH
from bs4 import BeautifulSoup

html = requests.get(url).text
soup = BeautifulSoup(html, 'lxml') # Parse the HTML as a string
    
tables = soup.find_all('table')
#tables = LH.fromstring(html).xpath('//table')
entries = pd.read_html('<html><body>{}</body></html>'.format(tables[0]))[0]
#entries['badge'] = [img.find('img')['src'] for img in tables[0].findAll("td", {"class": "final-results-icon"}) ]
#entries.dropna(how='all', axis=1, inplace=True)
entries.head()
```

```python
classes = [div.text for div in tables[0].findAll("div", {"class": "entry-class"}) ]
print(classes)
```

```python
row = tables[0].findAll("tr")[1]
cells= row.findAll('td')

entry = cells[1].text

divs = cells[2].findAll('div')
driver = cleanString( divs[0].text )
driver_nationality = divs[1].find('img')['alt']
driver_location = divs[1].text.strip()
driver_club = cleanString( divs[2].text )
#entry, driver, driver_nationality, driver_location, driver_club

divs = cells[3].findAll('div')
navigator = cleanString( divs[0].text )
navigator_nationality = divs[1].find('img')['alt']
navigator_location = divs[1].text.strip()
navigator_club = cleanString( divs[2].text )
#navigator, navigator_nationality, navigator_location, navigator_club

divs = cells[4].findAll('div')
entry_name = cleanString( divs[0].text )
entry_size = 
entry_colur
entry_type = cleanString( divs[1].text )
entry_sponsor = cleanString( divs[2].text )
entry_name, entry_type, entry_sponsor
```

```python
df = pd.DataFrame()
for row in tables[0].findAll("tr"):
    cells= row.findAll('td')
    if not cells:
        continue

    entry = cells[1].text

    divs = cells[2].findAll('div')
    driver = cleanString( divs[0].text )
    driver_nationality = divs[1].find('img')['alt'] if divs[1].find('img') else ''
    driver_location = divs[1].text.strip()
    driver_club = cleanString( divs[2].text )
    #entry, driver, driver_nationality, driver_location, driver_club

    divs = cells[3].findAll('div')
    navigator =  divs[0].text
    navigator_nationality = divs[1].find('img')['alt'] if divs[1].find('img') else ''
    navigator_location = divs[1].text.strip()
    navigator_club = divs[2].text 
    #navigator, navigator_nationality, navigator_location, navigator_club

    divs = cells[4].findAll('div')
    entry_name =  divs[0].text
    entry_type = divs[1].text
    entry_sponsor = divs[2].text
    entry_name, entry_type, entry_sponsor
    
    entry_bits = entry_type.split('~')
    entry_cc = entry_bits[0]
    entry_colour = entry_bits[1]
    entry_reg = entry_bits[2]
    entry_year = entry_bits[3]
    
    df = pd.concat([df, pd.DataFrame([[entry, driver, driver_nationality, driver_location, driver_club,navigator,
                    navigator_nationality, navigator_location, navigator_club,
                   entry_name, entry_type, entry_sponsor, entry_cc, entry_colour, entry_reg,entry_year ]] )])
    
df.columns = ['Class', 'Driver','Driver_Nationality','Driver_Town','Driver_Clubs',
              'Co-driver','Co-driver_Nationality','Co-driver_Town','Co-driver_Clubs',
              'Car','Details', 'Sponsor','CC','Colour','Registration','Year']
df=df[1:]
df.head()
```

```python
df.to_csv('corbeau19_entries.csv',index=False)
```

```python
!ls *.csv
```

```python

```
