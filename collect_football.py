#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""API-Sport.ru collector v2.6.1: BOTH MODES COVER THE FULL MOSCOW DAY.
Python 3.10+, stdlib only. Place existing .env with API_KEY beside this file.
python collect_football.py                 # fast; all upcoming matches with odds
python collect_football.py --mode full     # all matches and individual player data
Default: NO 200-event cap, NO 15-minute cut-off, 8 workers, 5 shared RPS.
Fast retains all day cards/odds; enriches ALL upcoming priced events throughout
00:00-23:59 Moscow, including evening. Full enriches all discovered events.
Both share the same math/market analysis, coverage and unfiltered AI analysis report.
Full mode adds player data for AI context; no untrained player adjustment.
Optional --minutes N or --max-events N explicitly sacrifices coverage.
--date YYYY-MM-DD, --budget 50000, --workers 8, --rps 5, --fresh supported.
Provider quota/rate limits still apply. Complete-day collection may exceed 20 min.
Team history is prioritized and spread across start hours. Prices are refreshed
for selected upcoming events after enrichment. Network failures remain visible.
Odds requests bypass cache reads and writes on every run (old cache is ignored).
No elapsed-age limit within a collection run; receipt timestamps remain auditable.
Response archives are history only and never reused as a quote cache.
Unknown markets, periods, and player props retain raw data without invented p.
Football supported research comparisons: 1X2, totals, team totals, Asian lines,
handicaps, DNB, BTTS. Double-chance model prices retained; overlapping outcomes
are not normalized as a 3-way market. Hockey: 1X2 regulation comparisons only;
other bookmaker periods require explicit rules. No automatic betting.
Mathematics uses regulation scores, same-season histories, k=5 smoothing,
home/away samples. Fixed-model retrospective time-ordered validation is included:
Brier, log loss and reliability vs league-only baseline, not bookmaker ROI.
No fitted/trained model is claimed. Calculations are not betting recommendations. Model validation and market-rule
limitations remain explicit; downstream AI performs selection.
--math-only old.json recalculates offline without a key or API requests.
Final output: raw+math JSON and smaller *_AI_REPORT.json for AI (no full raw data).
.progress.json is only a counter. Preserve the main JSON for detailed audits.
Original snapshots/response archive persist; no large repeated bundle rewrites.
v2.6 adds 50-game team history, 120-day league windows, Power/Shin,
market reliability diagnostics, fitted Dixon-Coles challenger (football),
and approximate Gamma-component posterior intervals. No automatic selection.
--history-days 120 --uncertainty-draws 64; full: --history-detail-matches 5.
Run scripts sequentially with the same key; they share account quota/lock.
API docs: https://docs.api-sport.ru/
"""

SPORT='football'
SPORT_KEY='FOOTBALL_API_KEY'
OUTPUT_NAME='football'
"""API-Sport.ru client, stdlib only; one account quota for all sports."""
import json,os,time,hashlib,uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime,timezone,timedelta
from urllib.request import Request,build_opener,HTTPRedirectHandler
from urllib.error import HTTPError,URLError
from urllib.parse import urlencode
from contextlib import contextmanager
BASE='https://api.api-sport.ru';MSK=timezone(timedelta(hours=3))
def now():return datetime.now(timezone.utc).isoformat()
def write(path,data):
 """Stream JSON to an atomic temporary file; never build the full JSON string."""
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
 try:
  with tmp.open('w',encoding='utf-8',buffering=1024*1024) as fp:
   json.dump(data,fp,ensure_ascii=False,allow_nan=False,separators=(',',':'))
   fp.flush();os.fsync(fp.fileno())
  os.replace(tmp,path)
 except BaseException:
  try:tmp.unlink(missing_ok=True)
  except OSError:pass
  raise

def redact_in_place(value,secret):
 """Remove reflected credentials without serializing/copying an entire response."""
 if isinstance(value,str):return value.replace(secret,'[REDACTED]')
 if isinstance(value,list):
  for i,item in enumerate(value):value[i]=redact_in_place(item,secret)
 elif isinstance(value,dict):
  renamed=[]
  for k in value:
   value[k]=redact_in_place(value[k],secret)
   if isinstance(k,str) and secret in k:renamed.append(k)
  for k in renamed:value[k.replace(secret,'[REDACTED]')]=value.pop(k)
 return value

def load_key(path):
 for l in Path(path).read_text(encoding='utf-8-sig').splitlines():
  if '=' in l and l.split('=',1)[0].strip() in ('API_KEY','API_SPORT_RU_KEY'):
   v=l.split('=',1)[1].strip().strip('\"\'')
   if v:return v
 raise ValueError('API key missing')
class NoRedirect(HTTPRedirectHandler):
 def redirect_request(self,*a,**kw):return None
@contextmanager
def lock(path):
 try:path.mkdir()
 except FileExistsError:raise RuntimeError('RUN_LOCKED: another run or stale lock; remove .run.lock ONLY if neither collector is running')
 try:yield
 finally:path.rmdir()
class Client:
 def __init__(self,root,key,budget=50000,rps=5):
  self.mutex=threading.RLock();self.rate_lock=threading.Lock();self.next_start=0.0;self.rps=rps
  self.url_locks=[threading.Lock() for _ in range(256)]
  self.root=Path(root);self.key=key;self.budget=budget;self.calls=0;self.blocked=False;self.records=[]
  self.state=self.root/'state';self.state.mkdir(parents=True,exist_ok=True);self.cache=self.state/'cache';self.cache.mkdir(exist_ok=True)
  self.ledger_path=self.state/'quota.json';self.ledger=json.loads(self.ledger_path.read_text()) if self.ledger_path.exists() else {'used':0,'limit':50000,'resetAt':0};self.usage_verified=False
 def sync(self):
  r=self.get('/v2/account/usage',ttl=0,charged=False)
  if r['status']=='OK':
   d=r['data'];u=d.get('usage',{});reset=u.get('resetAt');limit=(d.get('plan') or {}).get('dailyRequestLimit')
   if not isinstance(reset,(int,float)) or not isinstance(u.get('today'),int):return r
   previous=self.ledger['used'] if self.ledger.get('resetAt')==reset else 0
   used=max(previous,u['today']);remaining=u.get('remaining')
   if isinstance(remaining,int) and isinstance(limit,int):used=max(used,limit-remaining)
   self.ledger={'used':used,'limit':min(50000,limit) if isinstance(limit,int) and limit >= 0 else 0,'resetAt':reset,'services':d.get('services',[])}
   self.usage_verified=True;write(self.ledger_path,self.ledger)
  return r
 def available(self):return time.monotonic()<getattr(self,'deadline',float('inf')) and self.usage_verified and not self.blocked and self.calls<self.budget and self.ledger['used']<self.ledger['limit'] and time.time()*1000<self.ledger['resetAt']
 def get(self,path,params=None,ttl=900,charged=True):
  identity=path+json.dumps(params or {},sort_keys=True)
  stripe=int(hashlib.sha256(identity.encode()).hexdigest()[:8],16)%len(self.url_locks)
  with self.url_locks[stripe]:
   return self._get(path,params,ttl,charged)
 def _get(self,path,params=None,ttl=900,charged=True):
  if not path.startswith('/v2/') or any(x in path for x in ('://','..','?','#')):raise ValueError('Invalid API path')
  url=BASE+path+('?' + urlencode(sorted((params or {}).items())) if params else '')
  ident=hashlib.sha256(url.encode()).hexdigest();cp=self.cache/(ident+'.json')
  # Explicit odds collection must always hit the provider, regardless of --fresh.
  odds_request=(str((params or {}).get('with_bk_odds','')).lower() in ('true','1')
                or 'odds_stage' in (params or {})
                or any('odds' in part.lower() for part in path.split('/')))
  if odds_request:ttl=0
  if charged and (self.blocked or time.monotonic()>=getattr(self,'deadline',float('inf'))):
   r={'source_url':url,'fetched_utc':now(),'status':'SKIPPED_TIME_LIMIT' if not self.blocked else 'SKIPPED_BLOCKED','http_status':None,'data':None,'cache_hit':False}
   self.records.append(r.copy());return r
  if ttl and cp.exists():
   try:
    r=json.loads(cp.read_text());age=time.time()-datetime.fromisoformat(r['fetched_utc']).timestamp()
   except (ValueError,KeyError,OSError):
    r={};age=ttl
   if 0<=age<ttl:
    r={**r,'cache_hit':True};self.records.append({k:v for k,v in r.items() if k!='data'});return r
  r={'source_url':url,'fetched_utc':now(),'http_status':None,'status':'SKIPPED_QUOTA','data':None,'cache_hit':False}
  if not charged and path!='/v2/account/usage':raise ValueError('Only usage is quota-free')
  # A shared pacer limits starts across ALL worker threads.
  with self.rate_lock:
   delay=max(0.0,self.next_start-time.monotonic())
   if delay:time.sleep(delay)
   with self.mutex:
    if charged:
     if not self.available():
      self.records.append({k:v for k,v in r.items() if k!='data'});return r
     self.calls+=1;self.ledger['used']+=1;write(self.ledger_path,self.ledger)
    self.next_start=time.monotonic()+1/self.rps
  r['fetched_utc']=now()
  try:
   with build_opener(NoRedirect()).open(Request(url,headers={'Authorization':self.key,'Accept':'application/json','User-Agent':'APISportDesktop/2.3'}),timeout=50) as response:
    r['http_status']=response.status;raw=response.read(80_000_001)
    if len(raw)>80_000_000:raise ValueError('RESPONSE_TOO_LARGE')
    r['data']=json.loads(raw);del raw;r['status']='OK'
    if isinstance(r['data'],dict) and r['data'].get('error'):r['status']='API_ERROR'
  except HTTPError as e:
   r.update(status='HTTP_ERROR',http_status=e.code)
   try:r['data']=json.loads(e.read(10000))
   except Exception:pass
   if e.code in (401,403,429):
    with self.mutex:self.blocked=True
  except (URLError,OSError,TimeoutError,ValueError) as e:r.update(status='NETWORK_OR_DECODE_ERROR',error_type=type(e).__name__)
  # Never persist request headers or credential strings, including reflected errors.
  r=redact_in_place(r,self.key)
  if r['status']=='OK' and ttl and not odds_request:write(cp,r)
  archive=self.root/'history'/'responses'/(ident+'_'+uuid.uuid4().hex+'.json');write(archive,r)
  r['record_id']=archive.stem;self.records.append({k:v for k,v in r.items() if k!='data'});return r

"""Lossless data remain in bundle; summaries never invent missing statistics."""
from datetime import datetime,timezone
from collections import Counter

def ts(m):
 v=m.get('startTimestamp');return v/1000 if isinstance(v,(int,float)) else None

def form(history,target,team_id):
 cutoff=ts(target);season=(target.get('season') or {}).get('id');tour=(target.get('tournament') or {}).get('id')
 eligible=[m for m in history if m.get('status')=='finished' and cutoff is not None and ts(m) is not None and ts(m)<cutoff and team_id in ((m.get('homeTeam') or {}).get('id'),(m.get('awayTeam') or {}).get('id'))]
 eligible=sorted({m['id']:m for m in eligible}.values(),key=ts,reverse=True)[:20]
 def summarize(ms):
  out={'matches':0,'wins':0,'draws':0,'losses':0,'goals_for':0,'goals_against':0,'home':0,'away':0,'match_ids':[],'provisional_result_ids':[]}
  for m in ms:
   home=(m.get('homeTeam') or {}).get('id')==team_id;score=regulation_score(m)
   if score is None:continue
   gf,ga=score if home else score[::-1]
   if not isinstance(gf,(int,float)) or not isinstance(ga,(int,float)):continue
   out['matches']+=1;out['wins' if gf>ga else 'losses' if gf<ga else 'draws']+=1;out['goals_for']+=gf;out['goals_against']+=ga;out['home' if home else 'away']+=1;out['match_ids'].append(m['id'])
   if (m.get('scoreStatus') or {}).get('final') is not True:out['provisional_result_ids'].append(m['id'])
  out['mean_goals_for']=out['goals_for']/out['matches'] if out['matches'] else None;out['mean_goals_against']=out['goals_against']/out['matches'] if out['matches'] else None
  return out
 same=[m for m in eligible if season is not None and tour is not None and (m.get('season') or {}).get('id')==season and (m.get('tournament') or {}).get('id')==tour]
 return {'team_id':team_id,'cutoff_timestamp':cutoff,'all_competitions':summarize(eligible),'same_tournament_season':summarize(same),'home':summarize([m for m in same if (m.get('homeTeam') or {}).get('id')==team_id]),'away':summarize([m for m in same if (m.get('awayTeam') or {}).get('id')==team_id]),'days_rest':(cutoff-ts(eligible[0]))/86400 if eligible else None,'status':'AVAILABLE_PARTIAL_HISTORY' if len(same)>=10 else 'INSUFFICIENT_SAME_SEASON_HISTORY','score_basis':'regulation_periods_only','historical_snapshot_verified':False}

def quality(m):
 fields={'statistics':m.get('matchStatistics'),'pregame':m.get('pregame'),'bookmaker_odds':m.get('oddsBk'),'base_odds':m.get('oddsBase'),'home_lineup':(m.get('homeTeam') or {}).get('lineup'),'away_lineup':(m.get('awayTeam') or {}).get('lineup'),'events':m.get('liveEvents')}
 return {k:'PRESENT' if v else 'EMPTY_OR_ABSENT' for k,v in fields.items()}

def odds_rows(m,sport,received):
 for bk,board in (m.get('oddsBk') or {}).items():
  if not isinstance(board,dict):continue
  for key,market in (board.get('markets') or {}).items():
   if not isinstance(market,dict):continue
   stage=market.get('stage');active=(board.get('isBettingActiveByStage') or {}).get(stage) if stage else None
   for outcome,stake in (market.get('stakes') or {}).items():
    if not isinstance(stake,dict):continue
    for line in (stake.get('lines') or []) if market.get('hasArgument') else [stake]:
     if not isinstance(line,dict):continue
     yield {'provider':'API-Sport.ru','sport':sport,'match_id':m['id'],'bookmaker':bk,'market_key':key,'market_name':market.get('name'),'unit':market.get('unit'),'stage':stage,'market_status':market.get('status'),'stage_active':active,'outcome':outcome,'argument':line.get('argument'),'odds':line.get('factor'),'suspended':line.get('suspended',stake.get('suspended')),'source_updated_ms':line.get('dateUpdateMs'),'source_changed_ms':line.get('changedAt'),'board_updated_ms':(board.get('updatedAtByStage') or {}).get(stage),'received_utc':received,'bet_eligible':False,'rules_verified':False}

import argparse
import sys


def get_key(env_path):
    for name in (SPORT_KEY, 'API_SPORT_RU_KEY', 'API_KEY'):
        if os.environ.get(name, '').strip():
            return os.environ[name].strip()
    values = {}
    if env_path.exists():
        for line in env_path.read_text(encoding='utf-8-sig').splitlines():
            if '=' in line and not line.lstrip().startswith('#'):
                k, v = line.split('=', 1)
                values[k.strip()] = v.strip().strip('\"\'')
    for name in (SPORT_KEY, 'API_SPORT_RU_KEY', 'API_KEY'):
        if values.get(name):
            return values[name]
    raise ValueError('API key missing: put API_KEY=your_key in .env beside script')


def season_pairs(data):
    if not isinstance(data, dict):
        return set()
    return {(t['id'], s['id']) for t in (data.get('tournaments') or [])
            if isinstance(t, dict) and t.get('id') is not None
            for s in (t.get('seasons') or [])
            if isinstance(s, dict) and s.get('id') is not None}


def collect(c, args, result, output):
    base = '/v2/' + SPORT
    result.update(schema_version='api-sport-desktop-2.3', provider='API-Sport.ru',
                  sport=SPORT, date_moscow=args.date, created_utc=now(), status='RUNNING',
                  matches=[], match_sources={}, teams={}, players={}, tournaments={},
                  match_extras={}, dictionaries={}, forms={}, gaps=[], coverage={},
                  limitations=['Only data supplied by API; absent data are not invented.',
                               'Odds are API snapshots, not confirmed bookmaker cash-desk prices.',
                               'Hockey current scores may include overtime/shootout; preserve periods.',
                               'Player squad membership is not a confirmed starting lineup.',
                               'Historical requests return present-day snapshots; no look-ahead-safe backtest implied.'])
    requests_since_sync = 0
    tasks = 0

    def get(path, params=None, ttl=900):
        nonlocal requests_since_sync, tasks
        if c.calls - requests_since_sync >= 100:
            c.usage_verified = False
            c.sync()
            requests_since_sync = c.calls
        r = c.get(path, params, ttl=0 if args.fresh else ttl)
        tasks += 1
        if tasks % 100 == 0:
            result['progress'] = {'tasks': tasks, 'requests': c.calls, 'updated_utc': now()}
            write(output.with_suffix('.progress.json'), result['progress'])
        return r

    def batch(jobs):
        """Only the coordinator changes result; workers handle isolated responses.
        Bounded waves allow quota refresh with NO requests in flight.
        """
        nonlocal requests_since_sync, tasks
        iterator = iter(jobs)
        while True:
            wave = []
            for _ in range(max(1, min(100, 100 - (c.calls - requests_since_sync)))):
                try: wave.append(next(iterator))
                except StopIteration: break
            if not wave: return
            if c.calls - requests_since_sync >= 100:
                c.usage_verified = False
                c.sync()
                requests_since_sync = c.calls
            pool = ThreadPoolExecutor(max_workers=getattr(args, 'workers', 8))
            futures = {}
            try:
                for target, key, path, params, ttl in wave:
                    futures[pool.submit(c.get, path, params, 0 if args.fresh else ttl)] = (target, key)
                for future in as_completed(futures):
                    target, key = futures[future]
                    target[key] = future.result()
                    tasks += 1
            except BaseException:
                # Stop new starts, wait for already running HTTP requests, retain results.
                c.blocked = True
                for future in futures: future.cancel()
                pool.shutdown(wait=True, cancel_futures=True)
                for future, (target, key) in futures.items():
                    if future.done() and not future.cancelled() and future.exception() is None:
                        target[key] = future.result()
                raise
            finally:
                pool.shutdown(wait=True, cancel_futures=True)
            result['progress'] = {'tasks': tasks, 'requests': c.calls, 'updated_utc': now()}
            write(output.with_suffix('.progress.json'), result['progress'])
            cache_count=sum(bool(r.get('cache_hit')) for r in c.records)
            skipped=sum(str(r.get('status','')).startswith('SKIPPED_') for r in c.records)
            print(f'Tasks: {tasks}; API requests: {c.calls}; cache: {cache_count}; skipped: {skipped}', flush=True)

    result['usage_before'] = c.sync()
    if not c.usage_verified:
        result['status'] = 'QUOTA_UNVERIFIED'
        return
    services = c.ledger.get('services') or []
    if services and SPORT not in services:
        result['status'] = 'SPORT_NOT_IN_SUBSCRIPTION'
        return
    seen, cursor, match_ids = set(), '', set()
    discovery_complete = False
    while True:
        r = get(base + '/matches', {'date': args.date, 'tz': 'Europe/Moscow',
                'cursor': cursor, 'page_size': 500, 'sort': 'asc',
                'with_pregame': 'true', 'with_bk_odds': 'true', 'odds_stage': 'all'}, 300)
        data = r.get('data')
        if r['status'] != 'OK' or not isinstance(data, dict) or not isinstance(data.get('matches'), list):
            result['gaps'].append({'scope': 'discovery', 'reason': r['status'] if r['status'] != 'OK' else 'INVALID_SCHEMA'})
            break
        for m in data['matches']:
            if not isinstance(m, dict) or m.get('id') is None:
                result['gaps'].append({'scope': 'discovery', 'reason': 'INVALID_MATCH'})
                continue
            if ts(m) is not None and datetime.fromtimestamp(ts(m), MSK).date().isoformat() != args.date:
                result['gaps'].append({'scope': 'discovery', 'reason': 'DATE_MISMATCH', 'id': m['id']})
                continue
            if m['id'] not in match_ids:
                match_ids.add(m['id'])
                result['matches'].append(m)
                result['match_sources'][str(m['id'])] = {k: r.get(k) for k in ('source_url', 'fetched_utc', 'cache_hit')}
        if 'nextCursor' not in data:
            result['gaps'].append({'scope': 'discovery', 'reason': 'MISSING_CURSOR'})
            break
        nxt = data['nextCursor']
        if nxt is None:
            discovery_complete = True
            break
        if not isinstance(nxt, str) or not nxt or nxt in seen or nxt == cursor:
            result['gaps'].append({'scope': 'discovery', 'reason': 'REPEATED_OR_INVALID_CURSOR'})
            break
        seen.add(nxt)
        cursor = nxt
    matches = result['matches']
    fast = getattr(args, 'mode', 'full') == 'fast'
    candidates = [m for m in matches if m.get('status')=='notstarted' and (ts(m) or 0)>time.time() and m.get('oddsBk')] if fast else matches
    selected = sorted(candidates, key=lambda m: (not bool(m.get('oddsBk')), ts(m) or 0, str(m['id'])))
    if args.max_events:
        selected = selected[:args.max_events]
    result['coverage'] = {'discovered': len(matches), 'discovery_complete': discovery_complete,
                          'selected_for_enrichment': len(selected),
                          'not_selected': len(matches)-len(selected), 'enrichment_scope': 'all' if not args.max_events else 'limited'}
    result['collection_mode'] = getattr(args,'mode','full')
    result['omitted_by_mode'] = ['individual_player_requests','transfers','team_profiles','h2h','in_game_maps'] if fast else []
    result['coverage']['day_scope_moscow'] = '00:00:00 through 23:59:59'
    result['coverage']['candidate_count_before_explicit_limit'] = len(candidates)
    result['coverage']['selected_start_times_utc'] = [datetime.fromtimestamp(ts(m),timezone.utc).isoformat() for m in selected if ts(m) is not None]
    result['match_quality'] = {str(m['id']): quality(m) for m in matches}
    for name in ('StatisticsDict', 'SeasonStatisticsDict', 'BkOddsDict'):
        result['dictionaries'][name] = get('/v2/' + name + '.json', ttl=86400)
    contexts = {}
    hourly = {}
    for m in selected:
        hour=datetime.fromtimestamp(ts(m),MSK).hour if ts(m) else 0
        hourly.setdefault(hour,[]).append(m)
    spread=[]
    while any(hourly.values()):
        for hour in sorted(hourly):
            if hourly[hour]:spread.append(hourly[hour].pop(0))
    for m in spread:
        pair = ((m.get('tournament') or {}).get('id'), (m.get('season') or {}).get('id'))
        for side in ('homeTeam', 'awayTeam'):
            team = m.get(side) or {}
            tid = team.get('id')
            if tid is not None:
                contexts.setdefault(tid, set())
                if None not in pair:
                    contexts[tid].add(pair)
                result['teams'].setdefault(str(tid), {'id': tid, 'name': team.get('name')})
    # Broad useful data first, expensive per-player endpoints last.
    for endpoint, params, ttl in (
        ('matches', {'last': 50, 'next': 10}, 21600),
        ('statistics/seasons', None, 21600), ('squad', None, 21600),
        ('profile', None, 86400), ('transfers', None, 86400)):
        if fast and endpoint in ('profile','transfers'):continue
        batch((result['teams'][str(tid)], endpoint, f'{base}/teams/{tid}/{endpoint}', params, ttl) for tid in contexts)
    # Complete date-window league queries avoid treating selected-team samples as full leagues.
    from datetime import timedelta
    end_day=min(datetime.strptime(args.date,'%Y-%m-%d').date(),datetime.now(MSK).date())-timedelta(days=1)
    result['league_history']={'matches':[],'windows':{},'lookback_days':args.history_days,
                              'complete':False,'scope':'Selected tournament-season pairs; requested dates only'}
    history_jobs=[]
    pairs=sorted({p for values in contexts.values() for p in values})
    for tour,season in pairs:
        remaining=args.history_days;stop=end_day
        while remaining>0:
            days=min(31,remaining);start=stop-timedelta(days=days-1)
            key=f'{tour}:{season}:{start}:{stop}'
            params={'tournament_id':str(tour),'season_id':season,'date_from':str(start),'date_to':str(stop),
                    'tz':'Europe/Moscow','status':'finished','cursor':'','page_size':500,'sort':'asc',
                    'fields':'id,status,startTimestamp,homeTeam,awayTeam,homeScore,awayScore,tournament,season,scoreStatus'}
            history_jobs.append((result['league_history']['windows'],key,base+'/matches',params,21600))
            stop=start-timedelta(days=1);remaining-=days
    batch(history_jobs)
    historical={};complete=bool(history_jobs)
    for _,key,path,params,ttl in history_jobs:
        r=result['league_history']['windows'].get(key) or {};seen_cursors=set();pages=0;count=0;done=False
        while True:
            payload=r.get('data')
            if r.get('status')!='OK' or not isinstance(payload,dict) or not isinstance(payload.get('matches'),list):break
            pages+=1
            for game in payload['matches']:
                if isinstance(game,dict) and game.get('id') is not None:
                    if str((game.get('tournament') or {}).get('id'))!=params['tournament_id'] or (game.get('season') or {}).get('id')!=params['season_id']:continue
                    if ts(game) is None:continue
                    day=datetime.fromtimestamp(ts(game),MSK).date().isoformat()
                    if not params['date_from']<=day<=params['date_to']:continue
                    historical[str(game['id'])]=game;count+=1
            if 'nextCursor' not in payload:break
            nxt=payload['nextCursor']
            if nxt is None:done=True;break
            if not isinstance(nxt,str) or not nxt or nxt in seen_cursors:break
            seen_cursors.add(nxt);r=get(path,{**params,'cursor':nxt},ttl)
        complete=complete and done
        result['league_history']['windows'][key]={'complete':done,'pages':pages,'accepted_records':count,
                                                  'last_status':r.get('status'),'source_url':r.get('source_url'),
                                                  'fetched_utc':r.get('fetched_utc')}
        if not done:result['gaps'].append({'scope':'league_history','window':key,'reason':'INCOMPLETE_WINDOW'})
    result['league_history'].update(matches=list(historical.values()),complete=complete,unique_matches=len(historical))
    # Full mode collects event evidence for the most recent historical matches, not future shotmaps.
    result['historical_details']={}
    if not fast:
        detail_ids=set()
        for team in result['teams'].values():
            payload=(team.get('matches') or {}).get('data') or {}
            recent=payload.get('last') or [] if isinstance(payload,dict) else []
            recent=sorted((m for m in recent if isinstance(m,dict) and ts(m) is not None and regulation_score(m) is not None),key=ts,reverse=True)
            detail_ids.update(m['id'] for m in recent[:args.history_detail_matches])
        detail_jobs=[]
        for mid in sorted(detail_ids):
            bucket=result['historical_details'].setdefault(str(mid),{})
            for endpoint in ('events','shotmap'):
                detail_jobs.append((bucket,endpoint,f'{base}/matches/{mid}/{endpoint}',None,86400))
        batch(detail_jobs)

    jobs = []
    for tid, pairs in contexts.items():
        team = result['teams'][str(tid)]
        available = season_pairs(team['statistics/seasons'].get('data'))
        team['season_statistics'] = {}
        for tour, season in sorted(pairs):
            key = f'{tour}:{season}'
            if (tour, season) in available:
                jobs.append((team['season_statistics'], key, f'{base}/teams/{tid}/statistics',
                    {'tournament_id': tour, 'season_id': season}, 21600))
            else:
                team['season_statistics'][key] = {'status': 'EXACT_SEASON_NOT_AVAILABLE', 'data': None,
                    'reason': 'Exact ID not confirmed; inspect statistics/seasons request status. No season substitution.'}
    batch(jobs)
    batch((result['tournaments'], f'{tour}:{season}', f'{base}/tournament/{tour}/seasons/{season}', None, 21600)
          for tour, season in sorted({p for pairs in contexts.values() for p in pairs}))
    jobs = []
    form_history={}
    for game in all_history(result).values():
        for side in ('homeTeam','awayTeam'):
            form_history.setdefault((game.get(side) or {}).get('id'),[]).append(game)
    for m in selected:
        mid = str(m['id'])
        extras = result['match_extras'].setdefault(mid, {})
        home, away = (m.get('homeTeam') or {}).get('id'), (m.get('awayTeam') or {}).get('id')
        result['forms'][mid] = {}
        for tid in (home, away):
            history = ((result['teams'].get(str(tid), {}).get('matches') or {}).get('data') or {})
            if isinstance(history, dict):
                result['forms'][mid][str(tid)] = form(form_history.get(tid,[]), m, tid)
        if not fast and home is not None and away is not None and home != away:
            jobs.append((extras, 'h2h', base + '/h2h', {'team_id_1': home, 'team_id_2': away, 'limit': 20}, 21600))
        if not fast and m.get('status') in ('finished', 'inprogress', 'interrupted', 'suspended', 'willcontinue'):
            endpoints = ['events', 'shotmap', 'momentum'] if SPORT == 'football' else ['events', 'shotmap']
            if SPORT == 'football':
                endpoints.append('average-positions')
            for endpoint in endpoints:
                jobs.append((extras, endpoint, f'{base}/matches/{m["id"]}/{endpoint}', None, 300))
        else:
            extras['in_game_analytics'] = {'status': 'NOT_REQUESTED_BEFORE_PLAY'}
    batch(jobs)
    if not args.no_players and not fast:
        for tid, pairs in contexts.items():
            squad = result['teams'][str(tid)]['squad'].get('data') or {}
            if not isinstance(squad, dict):
                continue
            for p in (squad.get('players') or []):
                if not isinstance(p, dict) or p.get('id') is None:
                    continue
                pid = str(p['id'])
                item = result['players'].setdefault(pid, {'id': p['id'], 'squad_team_ids': [], 'contexts': []})
                if tid not in item['squad_team_ids']:
                    item['squad_team_ids'].append(tid)
                item['contexts'] = sorted(set(tuple(x) for x in item['contexts']) | pairs)
        for endpoint, params, ttl in [('statistics/seasons', None, 86400),
                                       ('matches', {'limit': 20}, 21600), ('profile', None, 86400)]:
            batch((item, endpoint, f'{base}/players/{pid}/{endpoint}', params, ttl) for pid, item in result['players'].items())
        jobs = []
        for pid, item in result['players'].items():
            available = season_pairs(item['statistics/seasons'].get('data'))
            item['season_statistics'] = {}
            for tour, season in item['contexts']:
                key = f'{tour}:{season}'
                if (tour, season) in available:
                    jobs.append((item['season_statistics'], key, f'{base}/players/{pid}/statistics',
                        {'tournament_id': tour, 'season_id': season}, 21600))
                else:
                    item['season_statistics'][key] = {'status': 'EXACT_SEASON_NOT_AVAILABLE', 'data': None}
        batch(jobs)
    # Refresh snapshot at the end, while preserving the initial match data if refresh fails.
    by_id={str(m['id']):m for m in matches}
    refresh_ids=[m['id'] for m in selected if m.get('status')=='notstarted']
    result['odds_refresh']={'requested_events':len(refresh_ids),'updated_events':0,'failed_batches':0}
    for offset in range(0,len(refresh_ids),100):
        ids=refresh_ids[offset:offset+100]
        if c.calls-requests_since_sync>=100:
            c.usage_verified=False;c.sync();requests_since_sync=c.calls
        r=c.get(base+'/matches',{'ids':','.join(str(x) for x in ids),'with_bk_odds':'true','with_pregame':'true','odds_stage':'all'},ttl=0)
        payload=r.get('data') or {}
        returned=payload.get('matches') if isinstance(payload,dict) else None
        if r['status']!='OK' or not isinstance(returned,list):
            result['odds_refresh']['failed_batches']+=1;continue
        requested={str(x) for x in ids}
        for fresh in returned:
            mid=str(fresh.get('id')) if isinstance(fresh,dict) else ''
            if mid not in requested or mid not in by_id:continue
            # Replace the complete card, not just its price; status may have changed to live.
            by_id[mid].clear();by_id[mid].update(fresh)
            result['match_sources'][mid]={k:r.get(k) for k in ('source_url','fetched_utc','cache_hit')}
            result['odds_refresh']['updated_events']+=1
    result['match_quality']={str(m['id']):quality(m) for m in matches}
    result['player_enrichment_enabled'] = not args.no_players and not fast
    result['coverage']['by_hour_moscow']={}
    selected_ids={str(m['id']) for m in selected}
    for m in matches:
        hour=str(datetime.fromtimestamp(ts(m),MSK).hour) if ts(m) else 'unknown'
        bucket=result['coverage']['by_hour_moscow'].setdefault(hour,{'discovered':0,'with_odds':0,'selected':0,'both_team_histories_ok':0})
        bucket['discovered']+=1;bucket['with_odds']+=bool(m.get('oddsBk'))
        if str(m['id']) in selected_ids:
            bucket['selected']+=1
            bucket['both_team_histories_ok']+=all((result['teams'].get(str((m.get(side) or {}).get('id')),{}).get('matches') or {}).get('status')=='OK' for side in ('homeTeam','awayTeam'))
    result['status'] = 'COMPLETED'


def finalize(c, result):
    result['finished_utc'] = now()
    result['requests_made'] = c.calls
    result['quota'] = c.ledger
    result['request_log'] = c.records
    counts = dict(Counter(r['status'] for r in c.records))
    result['request_status_counts'] = counts
    result.setdefault('coverage', {})['successful_responses'] = counts.get('OK', 0)
    result['coverage']['skipped_requests'] = sum(v for k,v in counts.items() if k.startswith('SKIPPED_'))
    result['coverage']['failed_requests'] = sum(v for k,v in counts.items() if k != 'OK' and not k.startswith('SKIPPED_'))
    refresh=result.get('odds_refresh') or {}
    if result.get('status') == 'COMPLETED' and (bool(result.get('gaps')) or not result['coverage'].get('discovery_complete') or any(k != 'OK' for k in counts) or refresh.get('updated_events',0)<refresh.get('requested_events',0)):
        result['status'] = 'COMPLETED_WITH_GAPS'
    result['coverage']['provider_data_completeness'] = 'NOT_GUARANTEED: inspect raw fields, match_quality and per-endpoint statuses'


# Research calculations: no model validation or bookmaker-rule verification implied.
import math

def finite_number(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)

def regulation_score(m):
    if m.get('status') != 'finished' or (m.get('scoreStatus') or {}).get('final') is False:
        return None
    values=[]
    for side in ('homeScore','awayScore'):
        s=m.get(side) or {}; periods=[s.get('period'+str(i)) for i in range(1, 3 if SPORT=='football' else 4)]
        if not all(finite_number(x) and x>=0 and x==int(x) for x in periods):return None
        values.append(sum(periods))
    return tuple(values)

def poisson_matrix(lh,la):
    def pmf(lam):
        p=[math.exp(-lam)]
        for i in range(1,81):
            p.append(p[-1]*lam/i)
            if i>lam and 1-sum(p)<1e-12:break
        return p
    a,b=pmf(lh),pmf(la);mass=sum(a)*sum(b)
    return [(i,j,x*y/mass) for i,x in enumerate(a) for j,y in enumerate(b)],max(0,1-mass)

def payout_weights(matrix,family,selection,line=None):
    # win/loss represent fractions of a unit stake, so quarter lines work too.
    if line is not None and (not finite_number(line) or abs(line*4-round(line*4))>1e-8):
        raise ValueError('Line must be a finite multiple of 0.25')
    lines=[line]
    if line is not None and round(line*4)%2:
        lines=[math.floor(line*2)/2, math.ceil(line*2)/2]
    wins=losses=pushes=0.0
    for h,a,p in matrix:
        for ln in lines:
            if family=='result':v=1 if {'w1':h>a,'x':h==a,'w2':h<a}[selection] else -1
            elif family=='double_chance':v=1 if {'1x':h>=a,'x2':h<=a,'12':h!=a}[selection] else -1
            elif family=='btts':v=1 if ((h>0 and a>0)==(selection=='yes')) else -1
            elif family=='handicap':v=(h-a if selection=='home' else a-h)+ln
            elif family in ('total','home_total','away_total'):
                v=({'total':h+a,'home_total':h,'away_total':a}[family]-ln)*(1 if selection=='over' else -1)
            else:raise ValueError('Unsupported market')
            weight=p/len(lines)
            if v>1e-9:wins+=weight
            elif v< -1e-9:losses+=weight
            else:pushes+=weight
    return {'win_fraction':wins,'loss_fraction':losses,'push_fraction':pushes,
            'fair_odds':1+losses/wins if wins>1e-12 else None}

def attach_math(result):
    history=all_history(result)
    output={'version':'research-poisson-1.1','validated_model':False,'publication_enabled':False,
            'method':{'smoothing_k':5,'min_league_sample':20,'min_team_venue_sample':5,
                      'formula':'lambda_home = league_home * smoothed_home_GF/league_home * smoothed_away_GA/league_home; analogous away',
                      'score_basis':'sum of regulation periods only',
                      'sample_warning':'League mean uses requested league windows plus team histories; inspect window completeness, never assume full season.',
                      'adjustments':'No fitted corrections for injuries, goalkeeper, lineup, tactics or opponent strength.'},
            'matches':{},'source_history_count':len(history)}
    history_groups={}
    for m in history.values():
        key=((m.get('tournament') or {}).get('id'),(m.get('season') or {}).get('id'))
        history_groups.setdefault(key,[]).append(m)
    now_seconds=time.time()
    for target in result.get('matches',[]):
        mid=str(target['id']);out={'status':'DATA_INSUFFICIENT','bet_eligible':False}
        output['matches'][mid]=out
        if target.get('status')!='notstarted' or ts(target) is None or ts(target)<=now_seconds:
            out['reason']='NOT_AN_UPCOMING_PREMATCH';continue
        tour=(target.get('tournament') or {}).get('id');season=(target.get('season') or {}).get('id')
        if tour is None or season is None:out['reason']='MISSING_TOURNAMENT_SEASON';continue
        cutoff=min(now_seconds,ts(target));eligible=[]
        for m in history_groups.get((tour,season),[]):
            if ts(m) is None or ts(m)>=cutoff:continue
            if (m.get('tournament') or {}).get('id')!=tour or (m.get('season') or {}).get('id')!=season:continue
            score=regulation_score(m)
            if score is not None:eligible.append((m,score))
        eligible.sort(key=lambda x:ts(x[0]),reverse=True)
        ht=(target.get('homeTeam') or {}).get('id');at=(target.get('awayTeam') or {}).get('id')
        hs=[(m,s) for m,s in eligible if (m.get('homeTeam') or {}).get('id')==ht][:20]
        aw=[(m,s) for m,s in eligible if (m.get('awayTeam') or {}).get('id')==at][:20]
        out['sample_sizes']={'league':len(eligible),'home_at_home':len(hs),'away_away':len(aw)}
        if len(eligible)<20 or len(hs)<5 or len(aw)<5:
            out['reason']='INSUFFICIENT_SAME_SEASON_REGULATION_HISTORY';continue
        bh=sum(s[0] for _,s in eligible)/len(eligible);ba=sum(s[1] for _,s in eligible)/len(eligible)
        if min(bh,ba)<=0:out['reason']='ZERO_LEAGUE_BASELINE';continue
        hgf=(sum(s[0] for _,s in hs)+5*bh)/(len(hs)+5)
        hga=(sum(s[1] for _,s in hs)+5*ba)/(len(hs)+5)
        agf=(sum(s[1] for _,s in aw)+5*ba)/(len(aw)+5)
        aga=(sum(s[0] for _,s in aw)+5*bh)/(len(aw)+5)
        lh,la=hgf*aga/bh,agf*hga/ba
        if not 0<lh<=12 or not 0<la<=12:out['reason']='EXTREME_INTENSITIES';continue
        matrix,tail=poisson_matrix(lh,la)
        out.update(status='RESEARCH_ONLY',reason='UNVALIDATED_MODEL',lambda_home=lh,lambda_away=la,
                   league_home_mean=bh,league_away_mean=ba,tail_mass_omitted=tail,
                   history_match_ids=[m['id'] for m,_ in eligible],
                   unconfirmed_final_history_ids=[m['id'] for m,_ in eligible if (m.get('scoreStatus') or {}).get('final') is not True],
                   probabilities={k:sum(p for h,a,p in matrix if condition(h,a)) for k,condition in
                                  [('w1',lambda h,a:h>a),('x',lambda h,a:h==a),('w2',lambda h,a:h<a)]},
                   markets=[],bookmaker_comparison=[])
        specs=[('result',k,None) for k in ('w1','x','w2')]+[('double_chance',k,None) for k in ('1x','x2','12')]
        specs += [('btts',k,None) for k in ('yes','no')]+[('handicap',k,0) for k in ('home','away')]
        max_total=6 if SPORT=='football' else 10
        specs += [(family,side,n/2) for family in ('total','home_total','away_total') for n in range(1,max_total*2+1) for side in ('over','under')]
        specs += [('handicap',side,n/2) for n in range(-6,7) if n for side in ('home','away')]
        for family,side,line in specs:
            out['markets'].append({'family':family,'selection':side,'line':line,'period':'regulation',
                                   **payout_weights(matrix,family,side,line)})
        # A documented canonical 1X2 key is the only automatically compared market.
        # Other markets remain raw; ambiguous settlement periods are never guessed.
        received=(result.get('match_sources',{}).get(mid) or {}).get('fetched_utc')
        for bk,board in (target.get('oddsBk') or {}).items():
            if not isinstance(board,dict):continue
            market=(board.get('markets') or {}).get('result') or {}
            stakes=market.get('stakes') or {}
            row={'bookmaker':bk,'market_key':'result','period':'regulation','received_utc':received,
                 'status':'UNUSABLE_OR_INCOMPLETE','bet_eligible':False}
            out['bookmaker_comparison'].append(row)
            if market.get('stage')!='prematch' or market.get('status')!='open' or (board.get('isBettingActiveByStage') or {}).get('prematch') is not True:continue
            if set(stakes)!= {'w1','x','w2'}:continue
            prices={k:(stakes[k] or {}).get('factor') for k in ('w1','x','w2')}
            if any(not finite_number(v) or v<=1 for v in prices.values()) or any(stakes[k].get('suspended') for k in prices):continue
            margin=sum(1/v for v in prices.values())
            row.update(status='RESEARCH_COMPARISON_NOT_EXECUTABLE_PRICE',overround=margin-1,selections={})
            for k,odds in prices.items():
                p=out['probabilities'][k];market_p=(1/odds)/margin
                row['selections'][k]={'odds':odds,'p_model':p,'p_market_no_vig':market_p,'fair_odds':1/p if p else None,
                                      'edge':p-market_p,'ev':p*odds-1,'source_updated_ms':stakes[k].get('dateUpdateMs')}
        variations=[]
        for dh,da in [(.85,.85),(.85,1.15),(1.15,.85),(1.15,1.15)]:
            mat,_=poisson_matrix(lh*dh,la*da)
            variations.append({k:sum(p for h,a,p in mat if f(h,a)) for k,f in [('w1',lambda h,a:h>a),('x',lambda h,a:h==a),('w2',lambda h,a:h<a)]})
        out['sensitivity_not_confidence_interval']={k:[min(v[k] for v in variations),max(v[k] for v in variations)] for k in ('w1','x','w2')}
        out['sensitivity_assumption']='Independent ±15% lambda perturbations, engineering scenario only'
    output['coverage']=dict(Counter(v['status'] for v in output['matches'].values()))
    result['mathematics']=output




def result_probabilities(lh,la):
    matrix,_=poisson_matrix(lh,la)
    return [sum(p for h,a,p in matrix if h>a),sum(p for h,a,p in matrix if h==a),sum(p for h,a,p in matrix if h<a)]

def historical_validation(data):
    """Fixed model, chronological features; no parameter fitting on held-out data."""
    histories={k:m for k,m in all_history(data).items() if ts(m) is not None and ts(m)<time.time() and regulation_score(m) is not None}
    groups={}
    for m in histories.values():
        key=((m.get('tournament') or {}).get('id'),(m.get('season') or {}).get('id'))
        if None not in key:groups.setdefault(key,[]).append(m)
    rows=[]
    for key,games in groups.items():
        games.sort(key=ts)
        prior=[];home={};away={};sh=sa=0
        for m in games:
            stamp=ts(m)
            # Insert only games whose scheduled start is strictly earlier.
            # Outcomes are retrospective; actual result publication timestamps are unavailable.
            while prior and ts(prior[0])+(10800 if SPORT=='football' else 14400)<stamp:
                old=prior.pop(0);score=regulation_score(old);sh+=score[0];sa+=score[1]
                home.setdefault((old.get('homeTeam') or {}).get('id'),[]).append(score)
                away.setdefault((old.get('awayTeam') or {}).get('id'),[]).append(score)
            n=sum(len(v) for v in home.values())
            hs=home.get((m.get('homeTeam') or {}).get('id'),[])[-20:]
            aw=away.get((m.get('awayTeam') or {}).get('id'),[])[-20:]
            if n>=20 and len(hs)>=5 and len(aw)>=5 and sh>0 and sa>0:
                bh,ba=sh/n,sa/n
                lh=((sum(s[0] for s in hs)+5*bh)/(len(hs)+5))*((sum(s[0] for s in aw)+5*bh)/(len(aw)+5))/bh
                la=((sum(s[1] for s in hs)+5*ba)/(len(hs)+5))*((sum(s[1] for s in aw)+5*ba)/(len(aw)+5))/ba
                if 0<lh<=12 and 0<la<=12:
                    actual=regulation_score(m);y=0 if actual[0]>actual[1] else 1 if actual[0]==actual[1] else 2
                    p=result_probabilities(lh,la);b=result_probabilities(bh,ba)
                    rows.append({'match_id':m['id'],'timestamp':stamp,'league':key[0],'y':y,'p':p,'baseline':b,
                                 'lambda_home':lh,'lambda_away':la,'actual':actual,
                                 'brier':sum((v-(i==y))**2 for i,v in enumerate(p)),
                                 'log_loss':-math.log(max(p[y],1e-15)),
                                 'baseline_brier':sum((v-(i==y))**2 for i,v in enumerate(b)),
                                 'baseline_log_loss':-math.log(max(b[y],1e-15))})
            prior.append(m)
    rows.sort(key=lambda r:r['timestamp'])
    cut=rows[int(len(rows)*.7)]['timestamp'] if rows else None
    test=[r for r in rows if r['timestamp']>=cut] if rows else []
    def summarize(rs):
        return {'n':len(rs),**{k:sum(r[k] for r in rs)/len(rs) if rs else None for k in ['brier','log_loss','baseline_brier','baseline_log_loss']}}
    bins=[]
    for i in range(10):
        pairs=[(p,int(k==r['y'])) for r in test for k,p in enumerate(r['p']) if i/10<=p<(i+1)/10 or i==9 and p==1]
        bins.append({'range':[i/10,(i+1)/10],'n':len(pairs),'mean_p':sum(p for p,y in pairs)/len(pairs) if pairs else None,'frequency':sum(y for p,y in pairs)/len(pairs) if pairs else None})
    return {**market_validation_and_dc(rows),'status':'RETROSPECTIVE_DIAGNOSTIC_ONLY','fitted':False,'validated_for_betting':False,
            'all_walk_forward':summarize(rows),'last_30_percent_time_holdout':summarize(test),
            'reliability_holdout':bins,'holdout_match_ids':[r['match_id'] for r in test],
            'limitations':['Selected leagues/date windows plus current team histories, not an unbiased full-season archive.',
                            'Actual final-result publication time unavailable; scheduled start plus conservative 3h football/4h hockey lag.',
                            'No verified historical bookmaker prices: no market skill or ROI validation.',
                            'Fixed k=5; no parameter tuning or fitted player effects.'],
            'by_league_holdout':{str(k):summarize([r for r in test if r['league']==k]) for k in sorted(set(r['league'] for r in test))}}


def analysis_notes(row,model,data):
    """Auditable data and model limitations; no selection or ranking."""
    reasons=[]
    samples=model.get('sample_sizes') or {}
    if samples.get('league',0)<12 or min(samples.get('home_at_home',0),samples.get('away_away',0))<4:
        reasons.append('INSUFFICIENT_DATA')
    elif samples.get('league',0)<30 or min(samples.get('home_at_home',0),samples.get('away_away',0))<8:
        reasons.append('EXPERIMENTAL_LOW_SAMPLE')
    validation=data.get('validation') or {}
    if validation.get('validated_for_betting') is not True:
        reasons.append('PRELIMINARY_UNCALIBRATED')
    if row.get('settlement_rules_verified') is not True:
        reasons.append('SETTLEMENT_RULES_UNVERIFIED')
    if not row.get('received_utc'):
        reasons.append('UNKNOWN_QUOTE_RECEIPT_TIME')
    # Lineup information may be unavailable before kickoff; flag but never fabricate effects.
    reasons.append('LINEUP_EFFECT_NOT_MODELED')
    return reasons


def enrich_market_analysis(data):
    """Export every supported calculated outcome without recommendation tiers."""
    mathdata=data.get('mathematics') or {};index={str(m['id']):m for m in data.get('matches',[])}
    summary={'status':'ANALYSIS_ONLY','validated_model':False,'outcomes':[],
             'export_policy':{'version':'unified-1.0','selection_applied':False,
                              'max_snapshot_age_seconds':None,
                              'quote_age_policy':'NO_ELAPSED_AGE_LIMIT_WITHIN_COLLECTION_RUN',
                              'odds_cache_enabled':False},
             'message':'Все рассчитанные исходы без отбора и рейтинга. Рекомендации формирует нейросеть.',
             'math_same_in_fast_and_full':True,
             'player_data_effect':'Raw player/lineup evidence only; no untrained numerical adjustment.',
             'unsupported_markets':{},'market_counts':{'discovered':0,'evaluated':0,'skipped':0},'outcomes_evaluated':0}
    for mid,model in mathdata.get('matches',{}).items():
        match=index[mid];evaluations=[];model['market_evaluations']=evaluations
        if model.get('status')=='RESEARCH_ONLY':
            matrix,_=poisson_matrix(model['lambda_home'],model['lambda_away'])
            scenarios=[poisson_matrix(model['lambda_home']*h,model['lambda_away']*a)[0] for h,a in [(0.85,0.85),(.85,1.15),(1.15,.85),(1.15,1.15)]]
        else:matrix=None;scenarios=[]
        cache={}
        received=(data.get('match_sources',{}).get(mid) or {}).get('fetched_utc')
        try:age=time.time()-datetime.fromisoformat(received).timestamp()
        except (TypeError,ValueError):age=None
        for bk,board in (match.get('oddsBk') or {}).items():
            if not isinstance(board,dict):continue
            for key,market in (board.get('markets') or {}).items():
                summary['market_counts']['discovered']+=1
                reason=None
                if not isinstance(market,dict):reason='INVALID_SCHEMA'
                elif matrix is None:reason='NO_MODEL'
                elif market.get('stage')!='prematch' or market.get('status')!='open' or (board.get('isBettingActiveByStage') or {}).get('prematch') is not True:reason='INACTIVE_OR_NOT_PREMATCH'
                elif SPORT=='ice-hockey' and key not in ('result','double_chance'):reason='HOCKEY_SETTLEMENT_PERIOD_NOT_VERIFIED'
                specs={'result':('result',{'w1':'w1','x':'x','w2':'w2'}),
                       'double_chance':('double_chance',{'1x':'1x','x2':'x2','12':'12'}),
                       'btts':('btts',{'yes':'yes','no':'no'}),
                       'total':('total',{'over':'over','under':'under'}),
                       'asian_total':('total',{'over':'over','under':'under'}),
                       'total_team_1':('home_total',{'over':'over','under':'under'}),
                       'total_team_2':('away_total',{'over':'over','under':'under'}),
                       'handicap':('handicap',{'handicap_1':'home','handicap_2':'away'}),
                       'asian_handicap':('handicap',{'handicap_1':'home','handicap_2':'away'}),
                       'draw_no_bet':('handicap',{'w1':'home','w2':'away'})}
                if reason is None and key not in specs:reason='UNSUPPORTED_MODEL_OR_RULES'
                if reason:
                    summary['market_counts']['skipped']+=1
                    summary['unsupported_markets'][key+'|'+reason]=summary['unsupported_markets'].get(key+'|'+reason,0)+1
                    continue
                family,selections=specs[key];stakes=market.get('stakes') or {};group=[]
                if family=='result' and set(stakes)!={'w1','x','w2'}:
                    summary['market_counts']['skipped']+=1
                    summary['unsupported_markets'][key+'|NOT_COMPLETE_3_WAY']=summary['unsupported_markets'].get(key+'|NOT_COMPLETE_3_WAY',0)+1
                    continue
                hasline=family in ('total','home_total','away_total','handicap') and key!='draw_no_bet'
                if hasline and (market.get('unit')!='goals' or market.get('hasArgument') is not True):
                    summary['market_counts']['skipped']+=1;continue
                if not hasline and market.get('hasArgument') is True:
                    summary['market_counts']['skipped']+=1;continue
                if not set(stakes).issubset(selections):
                    summary['market_counts']['skipped']+=1;continue
                for outcome,stake in stakes.items():
                    if not isinstance(stake,dict):continue
                    for quote in (stake.get('lines') or []) if hasline else [stake]:
                        if not isinstance(quote,dict):continue
                        odds=quote.get('factor');line=quote.get('argument') if hasline else 0 if key=='draw_no_bet' else None
                        if not finite_number(odds) or odds<=1 or quote.get('suspended') or stake.get('suspended'):continue
                        if hasline and (not finite_number(line) or abs(line*4-round(line*4))>1e-8):continue
                        side=selections[outcome];ck=(family,side,line)
                        if ck not in cache:
                            w=payout_weights(matrix,family,side,line)
                            sw=[payout_weights(mat,family,side,line) for mat in scenarios]
                            cache[ck]=(w,sw)
                        w,sw=cache[ck];ev=w['win_fraction']*(odds-1)-w['loss_fraction']
                        worst=min(x['win_fraction']*(odds-1)-x['loss_fraction'] for x in sw)
                        row={'bookmaker':bk,'market_key':key,'family':family,'selection':outcome,'line':line,
                             'period':'regulation','odds':odds,**w,'ev':ev,'scenario_min_ev':worst,
                             'received_utc':received,'snapshot_age_seconds':age,'source_updated_ms':quote.get('dateUpdateMs'),
                             'p_market_no_vig':None,'edge':None,
                             'settlement_rules_verified':False,'model_validated':False}
                        group.append(row)
                # Normalize only mutually exclusive same-line binary markets or complete 1X2.
                for row in group:
                    if family=='double_chance':opponents=[] # Overlapping outcomes: no normalization.
                    elif family=='result':opponents=group if {x['selection'] for x in group}=={'w1','x','w2'} and len(group)==3 else []
                    else:
                        opposite=[x for x in group if x['selection']!=row['selection'] and
                                  x['line']==(-row['line'] if family=='handicap' else row['line'])]
                        opponents=[row]+opposite if len(opposite)==1 else []
                    if opponents:
                        invsum=sum(1/x['odds'] for x in opponents);q=(1/row['odds'])/invsum
                        methods=no_vig_methods([x['odds'] for x in opponents]);idx=next(i for i,x in enumerate(opponents) if x is row)
                        row['market_probabilities_by_method']={k:v[idx] for k,v in methods.items()}
                        resolved=row['win_fraction']+row['loss_fraction']
                        row['edge_by_method']={k:row['win_fraction']/resolved-v[idx] if resolved else None for k,v in methods.items()}
                        row.update(p_market_no_vig=q,overround=invsum-1,
                                   probability_basis='decisive_stake_fraction' if row['push_fraction'] else 'unconditional',
                                   edge=(row['win_fraction']/resolved-q) if resolved else None)
                    row.update(match_id=mid,home=(match.get('homeTeam') or {}).get('name'),
                               away=(match.get('awayTeam') or {}).get('name'),
                               start_timestamp=match.get('startTimestamp'),sample_sizes=model.get('sample_sizes'),
                               quality_notes=analysis_notes(row,model,data))
                    summary['outcomes'].append(row)
                evaluations.extend(group)
                summary['market_counts']['evaluated' if group else 'skipped']+=1
                summary['outcomes_evaluated']+=len(group)
    summary['outcome_count']=len(summary['outcomes'])
    data['analysis']=summary


# Literature-informed diagnostics. No fitted calibration or betting edge is implied.
def all_history(data):
    records={}
    for team in data.get('teams',{}).values():
        payload=(team.get('matches') or {}).get('data') or {}
        if isinstance(payload,dict):
            for m in payload.get('last') or []:
                if isinstance(m,dict) and m.get('id') is not None:records[str(m['id'])]=m
    for m in data.get('league_history',{}).get('matches',[]):
        if isinstance(m,dict) and m.get('id') is not None:records[str(m['id'])]=m
    return records


def no_vig_methods(prices):
    """Complete mutually exclusive market only; never mix bookmakers or lines."""
    if len(prices)<2 or any(not finite_number(x) or x<=1 for x in prices):return {}
    q=[1/x for x in prices];s=sum(q)
    result={'multiplicative':[x/s for x in q]}
    lo,hi=0.,1.
    while sum(x**hi for x in q)>1:hi*=2
    for _ in range(70):
        mid=(lo+hi)/2
        if sum(x**mid for x in q)>1:lo=mid
        else:hi=mid
    result['power']=[x**((lo+hi)/2) for x in q]
    if s>=1:
        def shin(z):
            return [(math.sqrt(z*z+4*(1-z)*x*x/s)-z)/(2*(1-z)) for x in q]
        lo,hi=0.,1.-1e-8
        for _ in range(70):
            mid=(lo+hi)/2
            if sum(shin(mid))>1:lo=mid
            else:hi=mid
        vals=shin((lo+hi)/2)
        if abs(sum(vals)-1)<1e-6:result['shin']=vals
    return result


def quantile(values,q):
    values=sorted(values);pos=(len(values)-1)*q;i=int(pos);f=pos-i
    return values[i]*(1-f)+values[min(i+1,len(values)-1)]*f


def dc_matrix(lh,la,rho):
    matrix,_=poisson_matrix(lh,la)
    factors={(0,0):1-lh*la*rho,(0,1):1+lh*rho,(1,0):1+la*rho,(1,1):1-rho}
    if min(factors.values())<=0:return None
    return [(h,a,p*factors.get((h,a),1)) for h,a,p in matrix]


def market_validation_and_dc(rows):
    """Chronological holdout; rho fitted only on earlier 70%, per league."""
    cut=rows[int(len(rows)*.7)]['timestamp'] if rows else None
    train=[r for r in rows if r['timestamp']<cut];test=[r for r in rows if r['timestamp']>=cut]
    rho_models={}
    if SPORT=='football':
        for league in sorted({r['league'] for r in rows}):
            sample=[r for r in train if r['league']==league]
            if len(sample)<100:
                rho_models[str(league)]={'status':'INSUFFICIENT_TRAINING','n':len(sample)};continue
            best=(float('-inf'),0.)
            for rho in [i/100 for i in range(-20,21)]:
                total=0.;valid=True
                for r in sample:
                    h,a=r['actual'];lh,la=r['lambda_home'],r['lambda_away']
                    factors={(0,0):1-lh*la*rho,(0,1):1+lh*rho,(1,0):1+la*rho,(1,1):1-rho}
                    if min(factors.values())<=0:valid=False;break
                    total+=math.log(factors.get((h,a),1))
                if valid and total>best[0]:best=(total,rho)
            rho_models[str(league)]={'status':'FITTED_CHALLENGER_ONLY','n':len(sample),'rho':best[1],
                                    'training_end_exclusive_timestamp':cut,'grid':[-.2,.2,.01]}
    specs=[('result',x,None) for x in ('w1','x','w2')]+[('btts','yes',None)]
    specs += [('total','over',x) for x in ([1.5,2.5,3.5] if SPORT=='football' else [4.5,5.5,6.5])]
    groups={};dc_scores=[]
    for r in test:
        matrix,_=poisson_matrix(r['lambda_home'],r['lambda_away']);actual=r['actual']
        for family,side,line in specs:
            p=payout_weights(matrix,family,side,line)['win_fraction']
            y=payout_weights([(actual[0],actual[1],1.)],family,side,line)['win_fraction']
            groups.setdefault(f'{family}:{side}:{line}',[]).append((p,y))
        fitted=rho_models.get(str(r['league']),{})
        if fitted.get('rho') is not None:
            mat=dc_matrix(r['lambda_home'],r['lambda_away'],fitted['rho'])
            if mat:
                p=[sum(v for h,a,v in mat if check(h,a)) for check in (lambda h,a:h>a,lambda h,a:h==a,lambda h,a:h<a)]
                dc_scores.append({'league':r['league'],'brier':sum((v-(i==r['y']))**2 for i,v in enumerate(p)),
                                  'log_loss':-math.log(max(p[r['y']],1e-15)),
                                  'baseline_brier':r['brier'],'baseline_log_loss':r['log_loss']})
    diagnostics={}
    for key,pairs in groups.items():
        bins=[]
        for i in range(10):
            selected=[(p,y) for p,y in pairs if min(9,int(p*10))==i]
            bins.append({'range':[i/10,(i+1)/10],'n':len(selected),
                         'mean_p':sum(p for p,y in selected)/len(selected) if selected else None,
                         'frequency':sum(y for p,y in selected)/len(selected) if selected else None})
        n=len(pairs)
        diagnostics[key]={'n':n,'brier':sum((p-y)**2 for p,y in pairs)/n,
                          'log_loss':sum(-math.log(max(p if y else 1-p,1e-15)) for p,y in pairs)/n,
                          'ece_10_bins':sum(b['n']*abs(b['mean_p']-b['frequency']) for b in bins if b['n'])/n,
                          'reliability':bins,'calibrator_fitted':False}
    return {'by_market_holdout':diagnostics,'dixon_coles':{'models':rho_models,'holdout_n':len(dc_scores),
            'holdout_metrics':{k:sum(r[k] for r in dc_scores)/len(dc_scores) if dc_scores else None for k in ('brier','log_loss','baseline_brier','baseline_log_loss')},
            'validated_for_betting':False,'method':'Fit rho on earlier 70% only; fixed existing goal rates; no automatic model switch.'}}


def enrich_uncertainty(data):
    import random
    history=all_history(data);index={str(m['id']):m for m in data.get('matches',[])}
    draws=data.get('analysis_config',{}).get('uncertainty_draws',64)
    for mid,model in (data.get('mathematics') or {}).get('matches',{}).items():
        if model.get('status')!='RESEARCH_ONLY':continue
        match=index[mid];ht=(match.get('homeTeam') or {}).get('id');at=(match.get('awayTeam') or {}).get('id')
        games=[history[str(x)] for x in model.get('history_match_ids',[]) if str(x) in history]
        games.sort(key=ts,reverse=True)
        hs=[regulation_score(m) for m in games if (m.get('homeTeam') or {}).get('id')==ht][:20]
        aw=[regulation_score(m) for m in games if (m.get('awayTeam') or {}).get('id')==at][:20]
        hs=[x for x in hs if x is not None];aw=[x for x in aw if x is not None]
        mats=[];rates=[];bh=model['league_home_mean'];ba=model['league_away_mean']
        seed=int(hashlib.sha256((SPORT+mid+'gamma-v1').encode()).hexdigest()[:16],16);rng=random.Random(seed)
        if hs and aw and draws:
            # Independent component posterior approximation, conditional on league means.
            pars=[(sum(s[0] for s in hs)+5*bh,len(hs)+5),
                  (sum(s[1] for s in hs)+5*ba,len(hs)+5),
                  (sum(s[1] for s in aw)+5*ba,len(aw)+5),
                  (sum(s[0] for s in aw)+5*bh,len(aw)+5)]
            for _ in range(draws):
                hgf,hga,agf,aga=[rng.gammavariate(shape,1/rate) for shape,rate in pars]
                lh,la=hgf*aga/bh,agf*hga/ba
                # Extreme draws are not silently truncated into a fake posterior.
                if max(lh,la)>30:continue
                mat,_=poisson_matrix(lh,la);mats.append(mat);rates.append((lh,la))
        model['uncertainty']={'method':'EMPIRICAL_BAYES_INDEPENDENT_GAMMA_COMPONENTS','requested_draws':draws,
                              'accepted_draws':len(mats),'seed':seed,'interval_level':.90,
                              'limitations':['Conditional on empirical league means; league-mean uncertainty omitted.',
                                             'Independent component approximation ignores overlap between team samples.',
                                             'Not an empirically calibrated confidence interval; no lineup/opponent adjustment.']}
        if mats:model['uncertainty']['lambda_intervals']={side:[quantile([r[i] for r in rates],.05),quantile([r[i] for r in rates],.95)] for i,side in enumerate(('home','away'))}
        fitted=(data.get('validation',{}).get('dixon_coles',{}).get('models',{}).get(str((match.get('tournament') or {}).get('id'))) or {})
        challenger=dc_matrix(model['lambda_home'],model['lambda_away'],fitted['rho']) if fitted.get('rho') is not None else None
        cached={}
        for row in model.get('market_evaluations',[]):
            key=(row['family'],row['selection'],row['line'])
            side={'handicap_1':'home','handicap_2':'away'}.get(row['selection'],row['selection'])
            if row['market_key']=='draw_no_bet':side={'w1':'home','w2':'away'}[side]
            if key not in cached:
                ws=[payout_weights(mat,row['family'],side,row['line']) for mat in mats]
                dc=payout_weights(challenger,row['family'],side,row['line']) if challenger else None
                cached[key]=(ws,dc)
            ws,dc=cached[key]
            if ws:
                evs=[w['win_fraction']*(row['odds']-1)-w['loss_fraction'] for w in ws]
                row['uncertainty']={'method':model['uncertainty']['method'],'draws':len(ws),'interval_level':.90,
                     'win_fraction_interval':[quantile([w['win_fraction'] for w in ws],q) for q in (.05,.95)],
                     'ev_interval':[quantile(evs,q) for q in (.05,.95)],
                     'mean_ev':sum(evs)/len(evs),'fraction_draws_ev_positive':sum(v>0 for v in evs)/len(evs),
                     'calibrated':False}
            if dc:row['dixon_coles_challenger']={**dc,'rho':fitted['rho'],'ev':dc['win_fraction']*(row['odds']-1)-dc['loss_fraction'],'validated':False}
    data['analysis']['uncertainty_method']='Approximate empirical Bayes, not validated intervals; see match limitations.'


def attach_analysis(data):
    attach_math(data)
    data['validation']=historical_validation(data)
    enrich_market_analysis(data)
    enrich_uncertainty(data)
    data['analysis_finished_utc']=now()


def export_decision_audit(path,report):
    """Append-only snapshot for downstream AI review, without inventing recommendations."""
    import csv
    import io
    import shutil
    directory=path.with_name(path.stem+'_audit')
    directory.mkdir(parents=True,exist_ok=True)
    outcomes=(report.get('analysis') or {}).get('outcomes') or []
    run_id=uuid.uuid4().hex[:12]
    prefix=directory / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+run_id)
    fields=['match_id','home','away','start_timestamp','bookmaker','market_key','selection','line','odds','win_fraction','loss_fraction','push_fraction','fair_odds','ev','scenario_min_ev','edge','quality_notes','received_utc','sample_sizes']
    with (prefix.with_suffix('.jsonl')).open('w',encoding='utf-8') as fp:
        for row in outcomes:
            fp.write(json.dumps({'run_id':run_id,**row},ensure_ascii=False,allow_nan=False)+'\n')
    with (prefix.with_suffix('.csv')).open('w',encoding='utf-8',newline='') as fp:
        writer=csv.DictWriter(fp,fieldnames=fields);writer.writeheader()
        for row in outcomes:
            writer.writerow({k:json.dumps(row.get(k),ensure_ascii=False) if isinstance(row.get(k),(dict,list)) else row.get(k) for k in fields})
    write(prefix.with_name(prefix.name+'_manifest.json'),{'run_id':run_id,'generated_utc':now(),'outcome_count':len(outcomes),'validated_for_betting':False,'source_report':path.stem+'_AI_REPORT.json','validation':report.get('validation'),'policy':(report.get('analysis') or {}).get('export_policy')})


def save_ai_report(path,data):
    # Compact but complete analysis, with links by match/team ID to full raw JSON.
    report={'schema_version':'ai-report-2.1','sport':data.get('sport'),'date_moscow':data.get('date_moscow'),
            'source_json':str(path.name),'coverage':data.get('coverage'),'validation':data.get('validation'),
            'analysis':data.get('analysis'),'mathematics':data.get('mathematics'),
            'analysis_config':data.get('analysis_config'),'league_history_coverage':{k:v for k,v in data.get('league_history',{}).items() if k!='matches'},
            'match_context':[{k:m.get(k) for k in ('id','startTimestamp','tournament','season','homeTeam','awayTeam','missingPlayers','pregame','referee')} for m in data.get('matches',[])],
            'forms':data.get('forms'),'gaps':data.get('gaps'),
            'collection_finished_utc':data.get('finished_utc'),'analysis_finished_utc':data.get('analysis_finished_utc')}
    write(path.with_name(path.stem+'_AI_REPORT.json'),report)
    export_decision_audit(path,report)
    a=data.get('analysis') or {}
    print(f"Analysis complete: {a.get('outcome_count',0)} calculated outcomes; no ranking or automatic selection",flush=True)



def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    root = Path(__file__).resolve().parent
    parser.add_argument('--date', default=datetime.now(MSK).date().isoformat())
    parser.add_argument('--env', type=Path, default=root / '.env')
    parser.add_argument('--budget', type=int, default=50000)
    parser.add_argument('--workers', type=int, default=8, help='Parallel workers, 1..32 (default 8)')
    parser.add_argument('--rps', type=float, default=5, help='Global request starts/second, 0.1..50 (default 5); follow provider limits')
    parser.add_argument('--history-days',type=int,default=120,help='League history window, default 120 days; 0 disables extra league requests')
    parser.add_argument('--history-detail-matches',type=int,default=5,help='Full mode: events and shotmap for latest N matches per team')
    parser.add_argument('--uncertainty-draws',type=int,default=64,help='Reproducible approximate posterior draws, 0 disables, default 64')
    parser.add_argument('--mode', choices=['fast','full'], default='fast')
    parser.add_argument('--minutes', type=float, default=None, help='Optional network time budget; default unlimited in BOTH modes; 0=unlimited')
    parser.add_argument('--math-only', type=Path, help='Recalculate an existing JSON offline, no API key needed')
    parser.add_argument('--max-events', type=int, default=None)
    parser.add_argument('--no-players', action='store_true')
    parser.add_argument('--fresh', action='store_true')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--data-dir', type=Path, default=root / 'api_sport_data')
    args = parser.parse_args(argv)
    if not 0<=args.history_days<=730 or not 0<=args.history_detail_matches<=50 or not 0<=args.uncertainty_draws<=512:
        parser.error('history-days: 0..730; history-detail-matches: 0..50; uncertainty-draws: 0..512')
    if args.math_only:
        data=json.loads(args.math_only.read_text(encoding='utf-8-sig'))
        if data.get('sport')!=SPORT:parser.error('Input sport does not match this script')
        data['analysis_config']={'uncertainty_draws':args.uncertainty_draws}
        attach_analysis(data)
        destination=args.output or args.math_only.with_name(args.math_only.stem+'_math.json')
        save_ai_report(destination,data);write(destination,data);print(f'Saved: {destination}');return 0
    if args.max_events is None:args.max_events=0
    if args.minutes is None:args.minutes=0
    try:
        datetime.strptime(args.date, '%Y-%m-%d')
        if not 1 <= args.budget <= 50000 or args.max_events < 0:
            raise ValueError('budget must be 1..50000; max-events >= 0')
        if not 1 <= args.workers <= 32 or not 0.1 <= args.rps <= 50:
            raise ValueError('workers must be 1..32; rps must be 0.1..50')
        if not math.isfinite(args.minutes) or args.minutes<0:raise ValueError('minutes must be finite and >= 0')
        key = get_key(args.env)
    except (ValueError, OSError) as e:
        parser.error(str(e))
    account_dir = args.data_dir / ('account_' + hashlib.sha256(key.encode()).hexdigest()[:24])
    account_dir.mkdir(parents=True, exist_ok=True)
    output = (args.output or args.data_dir / 'output' /
              f'{OUTPUT_NAME}_{args.date}_{datetime.now(timezone.utc):%H%M%S}_{uuid.uuid4().hex[:6]}.json').resolve()
    result = {}
    exit_code = 0
    try:
        with lock(account_dir / '.run.lock'):
            c = Client(account_dir, key, args.budget, args.rps)
            c.deadline=time.monotonic()+args.minutes*60 if args.minutes else float('inf')
            print(f'Sport: {SPORT}; Moscow date: {args.date}; output: {output}', flush=True)
            try:
                collect(c, args, result, output)
            except KeyboardInterrupt:
                result['status'] = 'INTERRUPTED_PARTIAL'
                exit_code = 130
            except Exception as e:
                result['status'] = 'FAILED_PARTIAL'
                result['error_type'] = type(e).__name__
                exit_code = 1
            finally:
                finalize(c, result)
                print('Calculating research mathematics...',flush=True)
                result['analysis_config']={'uncertainty_draws':args.uncertainty_draws,'history_days':args.history_days}
                try:attach_analysis(result)
                except Exception as e:result['mathematics']={'status':'FAILED','error_type':type(e).__name__}
                print('Saving AI_REPORT first (streaming)...',flush=True)
                save_ai_report(output,result)
                print('AI_REPORT saved. Saving full JSON (streaming)...',flush=True)
                write(output, result)
                print('Full JSON saved.',flush=True)
            print(f"Saved: {output}\nStatus: {result['status']}; matches: {len(result.get('matches', []))}; requests: {c.calls}")
            if result['status'] in ('QUOTA_UNVERIFIED', 'SPORT_NOT_IN_SUBSCRIPTION', 'COMPLETED_WITH_GAPS'):
                exit_code = exit_code or 2
    except (RuntimeError, OSError, ValueError) as e:
        detail=str(e) if isinstance(e,RuntimeError) else (f'{type(e).__name__}: errno={e.errno}, {e.strerror}' if isinstance(e,OSError) else type(e).__name__)
        print('Cannot start/save: '+detail,file=sys.stderr)
        return 1
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
