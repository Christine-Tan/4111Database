import pandas
result = sql select yearid, teamid, w, r, h, ab, 2b as b2, 3b as b3, hr, bb, hbp, era from lahman2017.teams;
df = result.DataFrame()