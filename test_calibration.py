import copy,importlib.util,json,shutil,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
ROOT=Path(__file__).parent
def runtime(sport='football'):
    spec=importlib.util.spec_from_file_location('test_support',ROOT/'test_support.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    m.SPORT='football' if sport=='football' else 'ice-hockey'
    ns=dict(m.__dict__);ns['attach_analysis']=lambda d:None;ns['main']=lambda argv:0
    exec(compile((ROOT/'calibration_runtime.py').read_text(),'calibration_runtime.py','exec'),ns)
    return SimpleNamespace(**ns)

def data(event=1,start=200):
    row={'bookmaker':'A','market_key':'result','family':'result','selection':'w1','line':None,'period':'regulation',
         'odds':1.6,'win_fraction':.7,'loss_fraction':.3,'push_fraction':0,'received_utc':'synthetic-test-time'}
    return {'matches':[{'id':event,'status':'notstarted','startTimestamp':start*1000,'homeTeam':{'id':11,'name':'H'},'awayTeam':{'id':12,'name':'A'},'tournament':{'id':9},'season':{'id':8}}],
       'mathematics':{'version':'test-v1','matches':{str(event):{'status':'RESEARCH_ONLY','probabilities':{'w1':.7,'x':.2,'w2':.1},'market_evaluations':[row,{**row,'bookmaker':'B'}]}}}}

def game(event=1):
    return {'id':event,'status':'finished','startTimestamp':200000,'homeTeam':{'id':11},'awayTeam':{'id':12},'tournament':{'id':9},'season':{'id':8},'scoreStatus':{'final':True},'homeScore':{'current':2,'period1':1,'period2':1,'period3':0},'awayScore':{'current':1,'period1':0,'period2':1,'period3':0}}

class Client:
    def __init__(self,games):self.games=games;self.calls=[]
    def available(self):return True
    def get(self,path,params,ttl):
        self.calls.append((path,params,ttl));return {'status':'OK','data':{'matches':self.games},'fetched_utc':'verified-test-time','source_url':'synthetic://test'}

class CalibrationTests(unittest.TestCase):
 def test_daily_persistence_dedup_and_cache_delete(self):
    m=runtime()
    with tempfile.TemporaryDirectory() as root:
        store=m.CalibrationStore(Path(root)/'BankrollCalibration','football');d=data()
        self.assertEqual(store.register(d,100)['new_event_snapshots'],1)
        self.assertEqual(store.register(d,110)['new_event_snapshots'],0)
        before=store.paths()[0].read_bytes();client=Client([game()])
        store.settle(client,asof=300);report=store.summarize();store.publish()
        self.assertEqual(report['counts']['settled'],1);metric=next(iter(report['by_model_league_market'].values()))
        self.assertEqual(metric['n'],1);self.assertAlmostEqual(metric['brier'],.09)
        (Path(root)/'api_sport_data').mkdir();shutil.rmtree(Path(root)/'api_sport_data')
        second=m.CalibrationStore(Path(root)/'BankrollCalibration','football')
        self.assertEqual(second.summarize()['counts']['settled'],1)
        second.settle(client,asof=400);self.assertEqual(len(client.calls),1)
        self.assertEqual(second.paths()[0].read_bytes(),before)
 def test_no_postmatch_registration(self):
    m=runtime()
    with tempfile.TemporaryDirectory() as root:self.assertEqual(m.CalibrationStore(root,'football').register(data(),201)['new_event_snapshots'],0)
 def test_pending_and_identity_conflict(self):
    m=runtime()
    for status,final,home in [('postponed',True,11),('finished',False,11),('finished',True,99)]:
     with tempfile.TemporaryDirectory() as root:
      st=m.CalibrationStore(root,'football');st.register(data(),100);g=game();g['status']=status;g['scoreStatus']['final']=final;g['homeTeam']['id']=home
      st.settle(Client([g]),asof=300);self.assertEqual(st.summarize()['counts']['pending'],1)
 def test_cancellation_excluded(self):
    m=runtime()
    with tempfile.TemporaryDirectory() as root:
     st=m.CalibrationStore(root,'football');st.register(data(),100);g=game();g['status']='canceled'
     st.settle(Client([g]),asof=300);r=st.summarize();self.assertEqual(r['counts']['cancelled_excluded'],1);self.assertEqual(r['by_model_league_market'],{})
 def test_quarter_line_not_binary_brier(self):
    m=runtime()
    with tempfile.TemporaryDirectory() as root:
     st=m.CalibrationStore(root,'football');d=data();rows=d['mathematics']['matches']['1']['market_evaluations']
     for r in rows:r.update(family='total',market_key='asian_total',selection='over',line=2.25,win_fraction=.5,loss_fraction=.3,push_fraction=.2)
     st.register(d,100);g=game();g['awayScore']['period2']=0
     st.settle(Client([g]),asof=300);rep=st.summarize()
     self.assertEqual(rep['settlement_counts_unique_markets']['HALF_LOSS'],1);self.assertEqual(rep['by_model_league_market'],{})
 def test_hockey_regulation_not_ot(self):
    m=runtime('hockey')
    with tempfile.TemporaryDirectory() as root:
     st=m.CalibrationStore(root,'ice-hockey');d=data()
     for r in d['mathematics']['matches']['1']['market_evaluations']:r['selection']='x'
     st.register(d,100);g=game();g['awayScore'].update(period1=1,period2=1,current=2);g['homeScore']['current']=3
     st.settle(Client([g]),asof=300);self.assertEqual(st.summarize()['settlement_counts_unique_markets']['WIN'],1)
 def test_corrupt_history_is_not_reset(self):
    m=runtime()
    with tempfile.TemporaryDirectory() as root:
     st=m.CalibrationStore(root,'football');bad=st.root/'predictions'/'broken.json';bad.write_text('{')
     with self.assertRaises(RuntimeError):st.summarize()
     self.assertEqual(bad.read_text(),'{')
 def test_fit_gate_and_train_test_isolation(self):
    m=runtime();rows=[{'timestamp':i*86400,'event_id':str(i),'p':.9,'y':int(i%5<3)} for i in range(400)]
    self.assertEqual(m._cal_fit(rows[:3])['status'],'ACCUMULATING')
    fit=m._cal_fit(rows);self.assertEqual(fit['status'],'ACTIVE_DIAGNOSTIC')
    changed=copy.deepcopy(rows)
    for r in changed[280:]:r['y']=0
    refit=m._cal_fit(changed);self.assertEqual((fit['a'],fit['b']),(refit['a'],refit['b']))
 def test_two_launches_through_hooks(self):
    import time
    from unittest.mock import patch
    base=runtime();ns={k:v for k,v in vars(base).items() if not k.startswith('_cal')}
    ns['TEST_DATA']=data(start=time.time()+100)
    ns['TEST_GAMES']=[]
    exec("class Client:\n def sync(self):\n  self.usage_verified=True;return {'status':'OK'}\n def available(self):return True\n def get(self,*a,**kw):return {'status':'OK','data':{'matches':TEST_GAMES}}\ndef attach_analysis(d):pass\ndef main(argv=None):\n c=Client();c.sync();attach_analysis(TEST_DATA);return 0\n",ns)
    exec((ROOT/'calibration_runtime.py').read_text(),ns)
    with tempfile.TemporaryDirectory() as temp:
     launch=time.time()
     args=['--calibration-dir',str(Path(temp)/'history'),'--data-dir',str(Path(temp)/'cache')]
     self.assertEqual(ns['main'](args),0)
     ns['TEST_DATA']=data(event=2,start=launch+1000);ns['TEST_GAMES']=[game()]
     with patch.object(ns['_ct'],'time',return_value=launch+200):self.assertEqual(ns['main'](args),0)
     summary=json.loads((Path(temp)/'history'/'football'/'summary.json').read_text())
     self.assertEqual(summary['counts']['settled'],1);self.assertEqual(summary['counts']['pending'],1)
 def test_installer_preserves_code_and_is_idempotent(self):
    spec=importlib.util.spec_from_file_location('installer',ROOT/'install_calibration.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    source="class Client:\n def sync(self):pass\ndef main(argv=None):return 0\ndef attach_analysis(data):pass\ndef regulation_score(x):pass\ndef payout_weights(*a):pass\nif __name__ == '__main__':\n main()\n"
    with tempfile.TemporaryDirectory() as root:
     root=Path(root)
     for name in ('collect_football.py','collect_hockey.py'):(root/name).write_text(source)
     m.install(root);before=(root/'collect_football.py').read_bytes();m.install(root)
     self.assertEqual(before,(root/'collect_football.py').read_bytes());self.assertEqual(len(list(root.glob('*.bak'))),2)
     for backup in root.glob('*.bak'):self.assertEqual(backup.read_text(),source)

if __name__=='__main__':unittest.main()
