import nltk 
import nltk.tokenize
import hunspell 
import simplejson as json
from couchdb import json as cdb_json
import codecs
from couchdb.multipart import read_multipart
from pprint import pprint 
import itertools
import networkx as nx


stem_engine = hunspell.HunSpell('data/de_DE_neu.dic', 'data/de_DE_neu.aff')
stopwords = nltk.corpus.stopwords.words('german')
stopwords.extend(["de", "hie", "eu", "gibt", "ganz", "all", "gen", "au", "dass", "wen", 
                  "16", "dr", "35", "50000", "90", "-,", ".\"", "fdp", "spd", "herr", "frau",
                  "linke", "cdu", "csu", "beschlussempfehlung", "bundesregierung", "wort",
                  "antrag", "gesetzentwurf", "drucksache", "bundestag", "ausschuss" ])
#print stopwords

abgeordnete_data = json.loads(codecs.open('abgeordnete.js', 'r', 'utf-8').read())
abgeordnete = []
#pprint(abgeordnete_data)
for abg in abgeordnete_data.get('data'):
    abgeordnete.append((abg.get('vorname').lower().split(" "), abg.get('nachname').lower().split("-")))
    
#print abgeordnete
    


class Speech(object):
    
    def __init__(self, data):
        self.data = data 
        self.items = None
        self._counts = None
        self._freqs = None
        self._nltk_text = None
        
    @property
    def text(self):
        return self.data.get('text', '')
    
    @property
    def nltk_text(self):
        if self._nltk_text is None:
            self._nltk_text = nltk.text.Text(self.text.encode('utf8'))
        return self._nltk_text
    
    @property
    def speaker(self):
        return self.data.get('speaker', {}).get('fullname', 'Konrad Adenauer')
    
    @property
    def party(self):
        return self.data.get('speaker', {}).get('party', 'APPD')
        
    @property
    def parliament(self):
        return self.data.get('parliament', {}).get('number', -1)
    
    @property
    def id(self):
        return self.data.get('speech_id', -1)
    
    @property
    def tokens(self):
        if self.items is None:
            self.items = []
            for word in nltk.tokenize.wordpunct_tokenize(unicode(self.text)):
                stems = self._stem(word.encode('utf8'))
                #stems = set([stem.lower() for stem in stems])
                if len(stems):
                    self.items.append(stems[0].lower())
                elif len(word) > 1:
                    self.items.append(word.encode('utf8').lower())
        return self.items
    
        
    @property
    def counts(self):
        if self._counts is None:
            self._counts = {}
            for tok in self.tokens:
                if tok in stopwords:
                    continue
                self._counts[tok] = self._counts.get(tok, 0) + 1
        return self._counts 
    
    @property    
    def freqs(self):
        if self._freqs is None:
            self._freqs = {}
            for (tok, n) in self.counts.items():
                self._freqs[tok] = float(n)/float(len(self))
        return self._freqs
                   
    def _stem(self, word):
        stem = tuple(stem_engine.stem(word))
        return stem
        
    def __len__(self):
        return len(self.tokens)
        
    def update_count(self, prev_counts):
        for k, v in self.counts.items():
            prev_counts[k] = prev_counts.get(k, 0) + v
        return prev_counts



def load_file(file):
    fh = codecs.open(file, 'r', 'utf-8')
    data = fh.read()
    fh.close()
    return json.loads(data)
    
    
def load_multipart_file(fn):
    fh = file(fn, 'r') #codecs.open(fn, 'r', 'utf-8')
    for headers, is_multipart, payload in read_multipart(fh):
        doc = cdb_json.decode(payload)
        #print "Reading speech", doc.get('speech_id'), "..."
        speech = Speech(doc)
        yield speech
        #speeches.append()
    fh.close()
    
def count_to_len(count):
    return float(sum(count.values()))
    
def count_to_freq(count):
    lens = count_to_len(count)
    freq = {}
    for k, v in count.items():
        freq[k] = float(v) / lens
    return freq
    
def freq_diff(freq_big, freq_small):
    diff = {}
    for k, v in freq_big.items():
        if k in freq_small:
            # 38 - 344
            diff[k] = (freq_small.get(k) - v) * 10000
    return diff

    
def print_party_dict(di, col, limit=100, reverse=True):
    di = sorted(di.items(), key=lambda (k, v): v, reverse=reverse)
    for (k, v), i in zip(di, range(limit)):
        print "%s:%s:%s" % (k, v, col)
        
def print_wordle_text(di, limit=30, reverse=True):
    di = sorted(di.items(), key=lambda (k, v): v, reverse=reverse)
    

def analyze():
    counts = {}
    party_counts = {}
    party_text = {}
    for s, i in itertools.izip(load_multipart_file('data/bundestagger_speeches.json'), itertools.count()):
        #if i > 1000: #s.parliament != 16: 
        #    break
        counts = s.update_count(counts)
        party_text[s.party] = party_text.get(s.party, '') + " " + s.text
        party_counts[s.party] = s.update_count(party_counts.get(s.party, {}))
        
    #pprint(counts)
    len_all = count_to_len(counts)
    len_party = {}
    freqs = count_to_freq(counts)
    party_freqs = {}
    party_diffs = {}
    party_colors = {
        'SPD': 'E31414',
        'CDU': '000000',
        'FDP': 'FFD61F',
        '90': '21C912',
        'LIN': 'DE28BC'
    }
    for party in party_counts.keys():
        color = 'FFFFFF'
        for p, v in party_colors.items():
            if p in party:
                color = v
        len_party[party] = count_to_len(party_counts.get(party))
        party_freqs[party] = count_to_freq(party_counts.get(party))
        party_diffs[party] = freq_diff(freqs, party_freqs[party])
        print "PARTY", party.encode('ascii', 'ignore'), "SPOKE", (len_party[party] / float(len_all)) * 100, "%"
        #print "..................................."
        #print print_party_dict(party_diffs[party], color)
        #print "..................................."
        #print print_party_dict(party_diffs[party], color, reverse=False)
        ptext = nltk.text.Text(party_text.get(party))
        print "COLLOCATIONS", ptext.collocations()
    #pprint(party_counts)
    
    
#analyze()

data = load_file('data/single.js')
#pprint(data)
speech = Speech(data)

def match_person(person, bigram):
    def _one(item, lst):
        for cand in lst:
            if item.encode('urf-8') == cand:
                return True
        return False
    return _one(bigram[0], person[0]) and \
           _one(bigram[1], person[1])
    

def find_mentions(tokens):
    for bgram in nltk.bigrams(tokens):
        print bgram
        for abg in abgeordnete:
            if match_person(abg, bgram):
                print "PSN", abg
        
    
find_mentions(speech.tokens)
#pprint(dir(speech.nltk_text)) #.collocations())
#toks = speech.tokens

#pprint(dir(nltk.collocations.BigramCollocationFinder.from_words(toks)))
#pprint(nltk.collocations.BigramCollocationFinder.from_words(toks).above_score())
#pprint(speech.tokens)
#pprint(speech.counts)
#pprint(speech.freqs)


#for tok in speech._tokenize("Ich bin eine Unternehmenssteuer!"):
#    print tok
        
# COlors: 

# SPD: E31414
# CDU: 000000
# FDP: FFD61F
# B90: 21C912
# LIN: DE28BC


