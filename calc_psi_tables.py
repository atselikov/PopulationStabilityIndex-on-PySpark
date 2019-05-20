from __future__ import division
import json
import sys
import numpy as np
import pandas as pd
from time import time
from math import *
from psi_settings import *
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
import re


# calculate population stabillity index 
# expected - etalon mounth stat
# actual - current mounth stat
# qlist - list of quantiles for calculating
def calc_psi(expected, actual, qlist):
        expected_len=expected.count()
        actual_len=actual.count()
        psi_tot=0
        for j in range(len(qlist)-1):
            actual_pct=actual.filter((actual.SCORE>=qlist[j]) & (actual.SCORE<qlist[j+1])).count() / actual_len
            expected_pct=expected.filter((expected.SCORE>=qlist[j]) & (expected.SCORE<qlist[j+1])).count() / expected_len
            if (actual_pct>0) and (expected_pct>0):
                psi_cut = (expected_pct-actual_pct)*np.log(expected_pct/actual_pct)
                psi_tot+=psi_cut
        return np.round(psi_tot,7)


if __name__ == "__main__":

	current_month = str(sys.argv[1])
	# actual month (to compare with benchmark month)
	print ('Current_month:', current_month) 

	# maincycle
	t0=time()
	statDF = []
	bank1_psiDF = []
	
	sc = SparkContext()
	hc = HiveContext(sc)
	
	for i in range(len(table_list)):
	    print ('**************', table_list[i], '\t', '**************', np.round((time()-t0)/60.,3))
	    cur_table=table_list[i]+current_month
	    init_table = table_list[i]+month_list[i]
	    
	    #1. rec count
	    SCR = hc.sql('select * from %s' %cur_table)
	    RecCount = SCR.count()
	    #print 'rec_count: ', RecCount
	    
	    #2. subs_key distinct
	    subs_key_distinct = SCR.select('subs_key').distinct().count()
	    #print 'subs_key distinct: ',  subs_key_distinct
	    
	    #3. null count
	    NullCount = SCR.filter(SCR['score_ball'].isNull()).count()\
	                        +  SCR.filter(SCR['score_ball']=='')\
	                        .count()
	    
	    #4. max, min, avg score    
	    SCRstat = SCR.withColumn('SCORE_DOUBLE', regexp_replace('score_ball', ',', '.').cast("double"))

	        
	    score_max = np.round(SCRstat.agg({"SCORE_DOUBLE": "max"}).collect()[0][0],num2round)
	    score_min = np.round(SCRstat.agg({"SCORE_DOUBLE": "min"}).collect()[0][0],num2round)
	    score_avg = np.round(SCRstat.agg({"SCORE_DOUBLE": "avg"}).collect()[0][0],num2round)
	    #print 'max: ', score_max, 'min: ', score_min, 'avg: ', score_avg
	    
	    #5. PSI
	    expected = hc.sql("SELECT regexp_replace(score_ball,',','.') as SCORE from %s order by SCORE" %init_table).cache()
	    actual = hc.sql("SELECT regexp_replace(score_ball,',','.') as SCORE from %s order by SCORE" %cur_table).cache()
	    qlist = list(np.linspace(0,1,groups_count+1))
	    psi_tot = calc_psi(expected, actual, qlist)
	    print ('psi: ', psi_tot)
	    
	    statDF.append([table_list[i], 
	                       RecCount,
	                       subs_key_distinct,
	                       NullCount,
	                       score_max,
	                       score_min,
	                       score_avg,
	                       psi_tot]) 

	# save psi's
	statDFpd = pd.DataFrame(statDF, columns=('table', 'RecCount','subs_key_distinct','NullCount','score_max','score_min','score_avg','psi'))

    # write report file
	writer = pd.ExcelWriter('PSI_CALCULATE'+current_month+'.xlsx')
	statDFpd.to_excel(writer,'ALL')
	writer.save()
