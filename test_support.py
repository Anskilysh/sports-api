"""Existing collector payout/score routines for offline regression tests only."""
import math
SPORT="football"
class Client:
 def sync(self):return {}
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

