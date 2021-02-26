#NOT FUNCTIONAL
#custom class to handle translating state codes to full names or vice versa

from PhysEmpSpiders.services.routes import Routes
import nltk
from nltk.corpus import stopwords

#STATES CLASS
#@author: Rob Duff
#@Description: Takes in a state name, abbreviated or not, validates it, then returns the abbreivated form
#@Updated: 2-20-2021
class states():
    def getAbbrev(self, state):
        us_state_abbrev = {
            'alabama': 'AL',
            'alaska': 'AK',
            'american Samoa': 'AS',
            'arizona': 'AZ',
            'arkansas': 'AR',
            'california': 'CA',
            'colorado': 'CO',
            'connecticut': 'CT',
            'delaware': 'DE',
            'district of Columbia': 'DC',
            'florida': 'FL',
            'georgia': 'GA',
            'guam': 'GU',
            'hawaii': 'HI',
            'idaho': 'ID',
            'illinois': 'IL',
            'indiana': 'IN',
            'iowa': 'IA',
            'kansas': 'KS',
            'kentucky': 'KY',
            'louisiana': 'LA',
            'maine': 'ME',
            'maryland': 'MD',
            'massachusetts': 'MA',
            'michigan': 'MI',
            'minnesota': 'MN',
            'mississippi': 'MS',
            'missouri': 'MO',
            'montana': 'MT',
            'nebraska': 'NE',
            'nevada': 'NV',
            'new Hampshire': 'NH',
            'new Jersey': 'NJ',
            'new Mexico': 'NM',
            'new York': 'NY',
            'north Carolina': 'NC',
            'north Dakota': 'ND',
            'northern Mariana Islands':'MP',
            'ohio': 'OH',
            'oklahoma': 'OK',
            'oregon': 'OR',
            'pennsylvania': 'PA',
            'puerto Rico': 'PR',
            'rhode Island': 'RI',
            'south Carolina': 'SC',
            'south Dakota': 'SD',
            'tennessee': 'TN',
            'texas': 'TX',
            'utah': 'UT',
            'vermont': 'VT',
            'virgin Islands': 'VI',
            'virginia': 'VA',
            'washington': 'WA',
            'west Virginia': 'WV',
            'wisconsin': 'WI',
            'wyoming': 'WY'
        }
        #check if state is already in abbreviated form
        if(len(state) > 2):
            abbrev = ''
            try:
                abbrev = us_state_abbrev[str(state).lower().strip()]
            except:
                abbrev = ''
        else:
            #flips the dictionary so short form is searchable
            abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))
            #confirm abbreviation exists in the dict
            if state in abbrev_us_state:
                abbrev = state
            else:
                abbrev = ''
        return abbrev

#SpecialtyDetection Class
#@Author: Rob Duff
#@desscription: when given a job title and specialty returns a valid specialty matching what it believes to be the specialty. 90%+ accuracy\
#@Updated: 2-24-2021
class SpecialtyDetection():
    def returnSpecialty(self, title, specialty):
        #set up stopwords
        more_stopwords = set(())
        stoplist = set(stopwords.words('english')) | more_stopwords
        route = Routes()
        Specialty_Id = None #default return value
        nursing = False
        if specialty is not None:
            exactMatch = route.spSelectSpecialty(specialty) #returns exact matches to the given specialty
            if exactMatch is not None:
                Specialty_Id = exactMatch[0]
            else:
                matching = []
                words = []
                #add words in specialty to list of words to check
                for w in str(specialty).replace(":",'').replace(',','').replace("'","").replace("/"," ").replace('-', '').strip().split(' '):
                    w = w.strip()
                    if w == 'Nursing':
                        nursing = True
                    if w != "":
                        words.append(w)
                #add words in title to list of words to check
                for w in str(title).replace(":",'').replace(',','').replace("'","").replace("/"," ").replace('-', '').strip().split(' '):
                    w = w.strip()
                    if w == 'Nursing':
                        nursing = True
                    if w not in stoplist and w != "" and w is not None:
                        words.append(w)
                #for all the words we have
                for w in words:
                    matches = route.spSelectSpecialtyLike(w) #return any speicialty like the given word
                    if matches is not None:
                        for i in matches:
                            if i not in matching:
                                matching.append(i)
                found = []
                if matching is not None and nursing == False:
                    #for each matching specialty given them a score based on how many word match our specialty
                    for m in matching:
                        cnt = 0
                        for w in words:
                            #if exact match given it a big score
                            if w == m[1] or w == m[2]:
                                cnt += 10
                            elif w in m[1]:
                                cnt += 1
                            elif m[2] is not None and w in m[2]:
                                cnt += 1
                        found.append([m[0], cnt])
                    #check system same as Hospital Checker
                    out = ["None", 0]
                    physician = None
                    if len(found) > 1:
                        for f in found:
                            if f[0] == 148 or f[0] == 196:
                                physician = f[0]
                            elif f[1] > out[1]:
                                out[0] = f[0]
                                out[1] = f[1]
                        if out[1] is None and physician is not None:
                            Specialty_Id = physician
                        elif out[1] > 0:
                            Specialty_Id = out[0]
                            #print("cnt is :" + str(out[1]))
                    elif len(found) == 1:
                        for f in found:
                            out[0] = f[0]
                            out[1] = f[1]
                            Specialty_Id = out[0]
        return Specialty_Id    