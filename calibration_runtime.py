"""Local prospective calibration, stdlib only. Embedded into each collector by installer."""
import json as _cj
import math as _cm
import os as _co
import time as _ct
import hashlib as _ch
import uuid as _cu
from pathlib import Path as _CP
from contextlib import contextmanager as _ccm

_CAL_VERSION='prospective-1.0'
_CAL_CONTEXT=None

def _cal_write(path,value):
    path=_CP(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+'.'+_cu.uuid4().hex+'.tmp')
    try:
        with tmp.open('w',encoding='utf-8',buffering=262144) as fp:
            _cj.dump(value,fp,ensure_ascii=False,allow_nan=False,separators=(',',':'));fp.flush();_co.fsync(fp.fileno())
        _co.replace(tmp,path)
    except BaseException:
        tmp.unlink(missing_ok=True);raise

@_ccm
def _cal_lock(directory):
    directory.mkdir(parents=True,exist_ok=True)
    fp=(directory/'process.lock').open('a+b')
    try:
        if _co.name=='nt':
            import msvcrt
            if fp.tell()==0:fp.write(b'0');fp.flush()
            fp.seek(0);msvcrt.locking(fp.fileno(),msvcrt.LK_NBLCK,1)
        else:
            import fcntl
            fcntl.flock(fp.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    except OSError:
        fp.close();raise RuntimeError('CALIBRATION_LOCKED: another collector for this sport is running')
    try:yield
    finally:
        if _co.name=='nt':
            fp.seek(0);msvcrt.locking(fp.fileno(),msvcrt.LK_UNLCK,1)
        else:fcntl.flock(fp.fileno(),fcntl.LOCK_UN)
        fp.close()

def _cal_hash(value):
    return _ch.sha256(_cj.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def _cal_read(path):
    # Fail visibly on corrupt durable history; never reset it to an empty history.
    try:
        with path.open(encoding='utf-8') as fp:return _cj.load(fp)
    except (ValueError,OSError) as exc:raise RuntimeError('CALIBRATION_HISTORY_UNREADABLE: '+str(path)) from exc

def _cal_side(row):
    side={'handicap_1':'home','handicap_2':'away'}.get(row['selection'],row['selection'])
    if row.get('market_key')=='draw_no_bet':side={'w1':'home','w2':'away'}.get(side,side)
    return side

def _cal_group(snapshot,row):
    return _cj.dumps([snapshot['model_version'],snapshot['league_id'],row['family'],_cal_side(row),row.get('line'),row.get('period')],separators=(',',':'))

def _cal_probability(row):
    # Brier/log-loss apply only to genuinely binary unconditional outcomes.
    p=row.get('win_fraction');push=row.get('push_fraction')
    if not isinstance(p,(int,float)) or not _cm.isfinite(p) or not 0<=p<=1:return None
    if not isinstance(push,(int,float)) or abs(push)>1e-10:return None
    line=row.get('line')
    if row['family'] in ('total','home_total','away_total','handicap'):
        if not isinstance(line,(int,float)) or abs(line*2-round(line*2))>1e-9 or round(line*2)%2!=1:return None
    return float(p)

def _cal_label(w):
    win,loss,push=w['win_fraction'],w['loss_fraction'],w['push_fraction']
    if win>.999999:return 'WIN'
    if loss>.999999:return 'LOSS'
    if push>.999999:return 'VOID'
    if win>0 and loss==0:return 'HALF_WIN'
    if loss>0 and win==0:return 'HALF_LOSS'
    return 'UNSUPPORTED'

class CalibrationStore:
    def __init__(self,root,sport):
        self.root=_CP(root)/sport;self.sport=sport;self.run_id=_cu.uuid4().hex
        for sub in ('predictions','results','reports','calibrators'): (self.root/sub).mkdir(parents=True,exist_ok=True)
        self.report={};self.models={};self.health=[]
    def paths(self):return sorted((self.root/'predictions').glob('*.json'))
    def register(self,data,recorded_at=None):
        stamp=_ct.time() if recorded_at is None else recorded_at
        matches={str(m['id']):m for m in data.get('matches',[])}
        mathdata=data.get('mathematics') or {};version=mathdata.get('version','UNKNOWN');added=0;skipped=0
        # One immutable first prematch snapshot per event/model version; all bookmaker prices retained.
        for mid,model in mathdata.get('matches',{}).items():
            match=matches.get(str(mid),{});start=match.get('startTimestamp')
            if model.get('status')!='RESEARCH_ONLY' or match.get('status')!='notstarted' or not isinstance(start,(int,float)) or start/1000<=stamp:
                skipped+=1;continue
            identity=_cal_hash([self.sport,str(mid),version]);path=self.root/'predictions'/(identity+'.json')
            if path.exists():continue
            rows=model.get('market_evaluations') or []
            if not rows:continue
            snapshot={'id':identity,'schema':_CAL_VERSION,'sport':self.sport,'event_id':str(mid),
                      'model_version':version,'created_timestamp':stamp,'start_timestamp_ms':start,
                      'home_id':(match.get('homeTeam') or {}).get('id'),'away_id':(match.get('awayTeam') or {}).get('id'),
                      'home':(match.get('homeTeam') or {}).get('name'),'away':(match.get('awayTeam') or {}).get('name'),
                      'league_id':(match.get('tournament') or {}).get('id'),'season_id':(match.get('season') or {}).get('id'),
                      'probabilities_1x2':model.get('probabilities'),'sample_sizes':model.get('sample_sizes'),
                      'source':(data.get('match_sources') or {}).get(str(mid)),
                      'outcomes':rows,'actual_stake':None,'monetary_pnl':None,
                      'note':'Generated prematch estimates, not proof of a placed bet or recommendation.'}
            _cal_write(path,snapshot);added+=1
        return {'new_event_snapshots':added,'not_saved_started_or_unmodeled':skipped}
    def settle(self,client,max_requests=20,asof=None):
        stamp=_ct.time() if asof is None else asof
        pending={}
        for path in self.paths():
            pred=_cal_read(path)
            if (self.root/'results'/path.name).exists():continue
            if pred['start_timestamp_ms']/1000>stamp:continue
            pending.setdefault(pred['event_id'],[]).append(pred)
        ids=list(pending);done=0;used=0
        for offset in range(0,len(ids),100):
            if used>=max_requests or not client.available():break
            group=ids[offset:offset+100];used+=1
            response=client.get('/v2/'+self.sport+'/matches',{'ids':','.join(group),
                  'fields':'id,status,startTimestamp,homeTeam,awayTeam,homeScore,awayScore,tournament,season,scoreStatus'},ttl=0)
            payload=response.get('data')
            if response.get('status')!='OK' or not isinstance(payload,dict) or not isinstance(payload.get('matches'),list):
                self.health.append({'status':response.get('status'),'http_status':response.get('http_status'),'scope':'results','requested':len(group)});continue
            returned={str(m.get('id')):m for m in payload['matches'] if isinstance(m,dict)}
            for event_id in group:
                game=returned.get(event_id)
                if game is None:self.health.append({'event_id':event_id,'status':'RESULT_NOT_RETURNED'});continue
                for pred in pending[event_id]:
                    path=self.root/'results'/(pred['id']+'.json')
                    if path.exists():continue
                    same=(pred['home_id'] is not None and pred['away_id'] is not None
                          and pred['home_id']==(game.get('homeTeam') or {}).get('id')
                          and pred['away_id']==(game.get('awayTeam') or {}).get('id')
                          and pred['league_id']==(game.get('tournament') or {}).get('id')
                          and pred['season_id']==(game.get('season') or {}).get('id'))
                    if not same:self.health.append({'event_id':event_id,'status':'IDENTITY_CONFLICT'});continue
                    status=game.get('status');score=regulation_score(game)
                    # Cancellation is excluded from evaluation, but bookmaker void rules are not guessed.
                    if status=='canceled':state='CANCELLED_EXCLUDED';score=None
                    elif status=='finished' and (game.get('scoreStatus') or {}).get('final') is True and score is not None:state='SETTLED'
                    else:self.health.append({'event_id':event_id,'status':'PENDING','provider_status':status});continue
                    settlements=[]
                    for index,row in enumerate(pred['outcomes']):
                        item={'outcome_index':index,'monetary_pnl':None,'bookmaker_settlement_verified':False}
                        if state!='SETTLED':item.update(status='CANCELLED_EXCLUDED',reason='No verified bookmaker cancellation rules')
                        elif row.get('period')!='regulation':item.update(status='UNSUPPORTED_PERIOD')
                        else:
                            try:
                                w=payout_weights([(score[0],score[1],1.)],row['family'],_cal_side(row),row.get('line'))
                                item.update(status=_cal_label(w),**w)
                            except (KeyError,ValueError,TypeError):item.update(status='UNSUPPORTED')
                        settlements.append(item)
                    _cal_write(path,{'prediction_id':pred['id'],'state':state,'event_id':event_id,
                            'checked_timestamp':stamp,'source_url':response.get('source_url'),
                            'fetched_utc':response.get('fetched_utc'),'score_regulation':score,
                            'provider_result':game,'outcomes':settlements,'settlement_basis':'MODEL_REGULATION_RULES_NOT_BOOKMAKER_TICKET'})
                    done+=1
        return {'result_requests':used,'newly_resolved_events':done,'due_events':len(ids),'request_limit':max_requests}
    def summarize(self):
        groups={};samples={};counts={'event_snapshots':0,'settled':0,'pending':0,'cancelled_excluded':0};labels={};multiclass={'n':0,'brier':0.,'log_loss':0.}
        for path in self.paths():
            pred=_cal_read(path);counts['event_snapshots']+=1;result_path=self.root/'results'/path.name
            if not result_path.exists():counts['pending']+=1;continue
            result=_cal_read(result_path)
            if result['state']!='SETTLED':counts['cancelled_excluded']+=1;continue
            counts['settled']+=1;seen=set()
            score=result['score_regulation'];probs=pred.get('probabilities_1x2') or {}
            if set(probs)=={'w1','x','w2'}:
                p=[probs[x] for x in ('w1','x','w2')];y=0 if score[0]>score[1] else 1 if score[0]==score[1] else 2
                if all(isinstance(v,(int,float)) and _cm.isfinite(v) and 0<=v<=1 for v in p) and abs(sum(p)-1)<1e-6:
                    multiclass['n']+=1;multiclass['brier']+=sum((v-(i==y))**2 for i,v in enumerate(p));multiclass['log_loss']-=_cm.log(max(p[y],1e-15))
            for item in result['outcomes']:
                row=pred['outcomes'][item['outcome_index']];key=_cal_group(pred,row)
                if key in seen:continue
                seen.add(key);labels[item['status']]=labels.get(item['status'],0)+1
                p=_cal_probability(row)
                if p is None or item['status'] not in ('WIN','LOSS'):continue
                calibrated=(row.get('local_calibration') or {}).get('probability')
                record={'p':p,'y':int(item['status']=='WIN'),'timestamp':pred['start_timestamp_ms']/1000,
                        'event_id':pred['event_id'],'calibrated_p':calibrated}
                _cal_accumulate(groups.setdefault(key,_cal_empty()),record)
                # Bounded recent training buffers, only two primary binary markets per league/model.
                if _cal_trainable(row):
                    import heapq
                    buffer=samples.setdefault(key,[])
                    entry=(record['timestamp'],record['event_id'],p,record['y'])
                    if len(buffer)<500:heapq.heappush(buffer,entry)
                    elif entry>buffer[0]:heapq.heapreplace(buffer,entry)
        metrics={key:_cal_finish(value) for key,value in groups.items()};models={}
        for key,buffer in samples.items():
            rows=[{'timestamp':t,'event_id':event,'p':p,'y':y} for t,event,p,y in sorted(buffer)]
            models[key]=_cal_fit(rows)
        self.models={k:v for k,v in models.items() if v['status']=='ACTIVE_DIAGNOSTIC'}
        model_record={'schema':_CAL_VERSION,'created_timestamp':_ct.time(),'models':models,'not_validated_for_betting':True}
        model_id=_cal_hash(model_record);_cal_write(self.root/'calibrators'/(model_id+'.json'),model_record)
        self.model_id=model_id
        self.report={'schema':_CAL_VERSION,'sport':self.sport,'directory':str(self.root),'counts':counts,
            'settlement_counts_unique_markets':labels,'by_model_league_market':metrics,
            'multiclass_1x2':{'n':multiclass['n'],'brier':multiclass['brier']/multiclass['n'] if multiclass['n'] else None,
                             'log_loss':multiclass['log_loss']/multiclass['n'] if multiclass['n'] else None},
            'active_calibration_groups':len(self.models),'calibrator_version':model_id,'health':self.health,
            'deduplication':'First snapshot per event/model; same model/league/family/side/line/period counts once across bookmakers.',
            'limitations':['Only actually recorded prematch estimates; no retrospective fabrication.',
             'Brier/log-loss exclude pushes and fractional payouts; payouts recorded separately.',
             'Cancelled matches excluded; cash settlement needs bookmaker rules.',
             'Different markets of one event are correlated; no pooled all-market accuracy.',
             'Calibration gate is an engineering policy, not proof of profitability.'],
            'monetary_pnl':None,'clv':None}
        return self.report
    def publish(self):
        _cal_write(self.root/'reports'/(self.run_id+'.json'),self.report)
        _cal_write(self.root/'summary.json',self.report)
    def apply(self,data):
        index={str(m['id']):m for m in data.get('matches',[])};version=(data.get('mathematics') or {}).get('version','UNKNOWN')
        for mid,model in (data.get('mathematics') or {}).get('matches',{}).items():
            pred={'model_version':version,'league_id':(index.get(str(mid),{}).get('tournament') or {}).get('id')}
            for row in model.get('market_evaluations',[]):
                fit=self.models.get(_cal_group(pred,row));p=_cal_probability(row)
                # Only binary complementary market families; independently changing 1X2 breaks normalization.
                if fit and p is not None and _cal_trainable(row):
                    q=_cal_transform(p,fit['a'],fit['b'])
                    row['local_calibration']={'probability':q,'fair_odds':1/q,'ev':q*row['odds']-1,
                       'calibrator_version':self.model_id,'trained_n':fit['train_n'],'holdout_n':fit['test_n'],
                       'status':'DIAGNOSTIC_ALTERNATIVE','original_probability':p,'validated_for_betting':False}

def _cal_trainable(row):
    return (row['family']=='btts' and row['selection']=='yes') or (row['family']=='total' and row['selection']=='over' and row.get('line')==(2.5 if SPORT=='football' else 5.5))

def _cal_empty():
    return {'n':0,'brier':0.,'log_loss':0.,'bins':[[0,0.,0.] for _ in range(10)],'cal_n':0,'cal_brier':0.,'cal_ll':0.}

def _cal_accumulate(acc,row):
    p,y=row['p'],row['y'];acc['n']+=1;acc['brier']+=(p-y)**2;acc['log_loss']-=_cm.log(max(p if y else 1-p,1e-15))
    bucket=acc['bins'][min(9,int(p*10))];bucket[0]+=1;bucket[1]+=p;bucket[2]+=y
    cp=row.get('calibrated_p')
    if isinstance(cp,(int,float)) and _cm.isfinite(cp) and 0<=cp<=1:
        acc['cal_n']+=1;acc['cal_brier']+=(cp-y)**2;acc['cal_ll']-=_cm.log(max(cp if y else 1-cp,1e-15))

def _cal_finish(acc):
    n=acc['n'];cn=acc['cal_n']
    return {'n':n,'unique_events':n,'brier':acc['brier']/n if n else None,'log_loss':acc['log_loss']/n if n else None,
            'reliability':[{'range':[i/10,(i+1)/10],'n':b[0],'mean_p':b[1]/b[0] if b[0] else None,'observed_frequency':b[2]/b[0] if b[0] else None} for i,b in enumerate(acc['bins'])],
            'prospective_calibrated':{'n':cn,'brier':acc['cal_brier']/cn if cn else None,'log_loss':acc['cal_ll']/cn if cn else None}}

def _cal_metrics(rows):
    n=len(rows);bins=[]
    for i in range(10):
        rs=[r for r in rows if min(9,int(r['p']*10))==i]
        bins.append({'range':[i/10,(i+1)/10],'n':len(rs),'mean_p':sum(r['p'] for r in rs)/len(rs) if rs else None,
                     'observed_frequency':sum(r['y'] for r in rs)/len(rs) if rs else None})
    return {'n':n,'unique_events':len({r['event_id'] for r in rows}),
            'brier':sum((r['p']-r['y'])**2 for r in rows)/n if n else None,
            'log_loss':sum(-_cm.log(max(r['p'] if r['y'] else 1-r['p'],1e-15)) for r in rows)/n if n else None,
            'reliability':bins,
            'prospective_calibrated':_cal_simple_scores([(r['calibrated_p'],r['y']) for r in rows if isinstance(r.get('calibrated_p'),(int,float))])}

def _cal_simple_scores(pairs):
    n=len(pairs)
    return {'n':n,'brier':sum((p-y)**2 for p,y in pairs)/n if n else None,
            'log_loss':sum(-_cm.log(max(p if y else 1-p,1e-15)) for p,y in pairs)/n if n else None}

def _cal_transform(p,a,b):
    p=min(1-1e-9,max(1e-9,p));z=a*_cm.log(p/(1-p))+b
    return 1/(1+_cm.exp(-max(-40,min(40,z))))

def _cal_fit(rows):
    # Calibration is separate from fitting the base sports model. Fixed policy, no daily hand tuning.
    out={'status':'ACCUMULATING','n':len(rows),'minimum_train':200,'minimum_test':100,'minimum_span_days':30,'maximum_recent_training_window':500}
    if len(rows)<300 or rows[-1]['timestamp']-rows[0]['timestamp']<30*86400:return out
    cutoff=rows[int(len(rows)*.7)]['timestamp'];train=[r for r in rows if r['timestamp']<cutoff];test=[r for r in rows if r['timestamp']>=cutoff]
    if len(train)<200 or len(test)<100 or len({r['y'] for r in train})<2:return out
    best=(float('inf'),1.,0.)
    for a in (.5,.75,1.,1.25,1.5):
        for b in (-.5,-.25,0.,.25,.5):
            loss=sum(-_cm.log(max(_cal_transform(r['p'],a,b) if r['y'] else 1-_cal_transform(r['p'],a,b),1e-15)) for r in train)/len(train)
            if loss<best[0]:best=(loss,a,b)
    _,a,b=best;base=_cal_simple_scores([(r['p'],r['y']) for r in test]);adjusted=_cal_simple_scores([(_cal_transform(r['p'],a,b),r['y']) for r in test])
    active=adjusted['log_loss']<base['log_loss']-.001 and adjusted['brier']<base['brier']-.001
    out.update(status='ACTIVE_DIAGNOSTIC' if active else 'NO_HOLDOUT_IMPROVEMENT',a=a,b=b,train_n=len(train),test_n=len(test),
               train_end_exclusive=cutoff,baseline_holdout=base,calibrated_holdout=adjusted)
    return out

_cal_original_main=main
_cal_original_attach=attach_analysis
_cal_original_sync=Client.sync

def _cal_sync(self):
    response=_cal_original_sync(self)
    context=_CAL_CONTEXT
    if context and not context['checked'] and self.usage_verified:
        context['checked']=True
        print('Checking saved forecasts and final results...',flush=True)
        stats=context['store'].settle(self,context['max_requests'])
        report=context['store'].summarize();report['result_check']=stats;context['store'].publish()
        print('Calibration: '+str(report['counts']),flush=True)
    return response

def _cal_attach(data):
    _cal_original_attach(data)
    context=_CAL_CONTEXT
    if context:
        store=context['store']
        if not context['checked']:
            store.health.append({'status':'RESULT_CHECK_NOT_AVAILABLE'})
            store.summarize()
        store.apply(data)
        saved=store.register(data);store.report['registration']=saved
        store.report['counts']['event_snapshots']+=saved['new_event_snapshots']
        store.report['counts']['pending']+=saved['new_event_snapshots']
        store.report['directory']=str(store.root)
        # Recount files only, keep metrics/model trained before today's predictions.
        store.report['recorded_event_files']=sum(1 for _ in store.paths())
        store.publish();data.setdefault('analysis',{})['local_calibration']=store.report

def main(argv=None):
    import argparse as _ca
    import sys as _cs
    global _CAL_CONTEXT
    extra=_ca.ArgumentParser(add_help=False)
    extra.add_argument('--calibration-dir',type=_CP,default=_CP.home()/'BankrollCalibration')
    extra.add_argument('--calibration-result-requests',type=int,default=20)
    args,remaining=extra.parse_known_args(_cs.argv[1:] if argv is None else argv)
    if '--help' in remaining or '-h' in remaining:
        print('Local calibration: --calibration-dir PATH (default ~/BankrollCalibration); --calibration-result-requests 20')
        return _cal_original_main(remaining)
    if any(x=='--math-only' or x.startswith('--math-only=') for x in remaining):
        # Offline recomputation is not a newly issued prematch prediction.
        return _cal_original_main(remaining)
    if not 0<=args.calibration_result_requests<=100:extra.error('calibration-result-requests must be 0..100')
    path=args.calibration_dir.expanduser().resolve()
    dp=_ca.ArgumentParser(add_help=False);dp.add_argument('--data-dir',type=_CP,default=_CP(__file__).resolve().parent/'api_sport_data')
    da,_=dp.parse_known_args(remaining);data_path=da.data_dir.expanduser().resolve()
    if path==data_path or data_path in path.parents:extra.error('Calibration directory must be OUTSIDE --data-dir')
    try:
        store=CalibrationStore(path,SPORT)
        with _cal_lock(store.root):
            _CAL_CONTEXT={'store':store,'checked':False,'max_requests':args.calibration_result_requests}
            print('Persistent calibration: '+str(store.root),flush=True)
            return _cal_original_main(remaining)
    except (OSError,RuntimeError) as exc:
        print('Calibration storage error: '+str(exc),file=_cs.stderr);return 1
    finally:_CAL_CONTEXT=None

Client.sync=_cal_sync
attach_analysis=_cal_attach
