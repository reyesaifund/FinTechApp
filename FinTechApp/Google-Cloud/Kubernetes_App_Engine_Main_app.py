import base64
import json
import logging

from flask import Flask, jsonify, request

from flask_cors import cross_origin

from six.moves import http_client

from numba import jit


app = Flask(__name__)

def _base64_decode(encoded_str):
    # Add paddings manually if necessary.
    num_missed_paddings = 4 - len(encoded_str) % 4
    if num_missed_paddings != 4:
        encoded_str += b'=' * num_missed_paddings
    return base64.b64decode(encoded_str).decode('utf-8')


@app.route('/echo', methods=['POST'])
def echo():
    """Simple echo service."""
    
    import json
    import requests
    from datetime import datetime, timedelta
    import numpy as np
    import time
    import ast
    import pandas as pd
    from pandas.tseries.offsets import BDay
    from pyearth import Earth
    import os, sys
    import pytz
    from sklearn import linear_model
    from sklearn.preprocessing import Imputer
    from math import ceil
    from scipy.stats import zscore
    from pandas.compat import StringIO
    sys.path.append('/usr/local/lib/python2.7/dist-packages')
    import h2o
    from dateutil.parser import parse as parse_date
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java"

    np.set_printoptions(suppress=True)

    
    def get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model_pyearth, quotes_side):
            
        der_y = np.take(section_price_hat, zero_crossings) 
        pct_chg = np.diff(der_y) / der_y[:-1]
        pct_chg_wh = np.where(pct_chg > 0)
        pct_chg_wh = pct_chg_wh[0]
        pct_chg_long = np.take(pct_chg, pct_chg_wh)
        avg_return_40_mins = np.nanmean(pct_chg_long)


        @jit(parallel=True)
        def for_0(pct_chg_long):
            result = np.array([])
            for pcl in range(0, len(pct_chg_long)-1):
                #pct_chg_long_2 = np.append(pct_chg_long_2, pcl)
                result = np.append(result, np.nanmean(pct_chg_long[:pcl]))
            return result
        running_avg_pcl = for_0(pct_chg_long)
        
        @jit(parallel=True)
        def for_1(running_avg_pcl):
            result = np.array([])
            for pcl in range(1, len(running_avg_pcl)-1):
                result = np.append(result, np.subtract(running_avg_pcl[pcl], running_avg_pcl[pcl-1]))
            return result
        avg_chg_in_avg_return = np.nanmean(for_1(running_avg_pcl))
        
        if 'avg_chg_in_avg_return' not in locals():
            avg_chg_in_avg_return = np.nan
        if zero_crossings.size > 5:
            return_minus_5 = pct_chg[-5]
            return_minus_4 = pct_chg[-4]
            return_minus_3 = pct_chg[-3]
            return_minus_2 = pct_chg[-2]
        elif zero_crossings.size == 5:
            return_minus_5 = np.nan
            return_minus_4 = pct_chg[-4]
            return_minus_3 = pct_chg[-3]
            return_minus_2 = pct_chg[-2]
        elif zero_crossings.size == 4:
            return_minus_5 = np.nan
            return_minus_4 = np.nan
            return_minus_3 = pct_chg[-3]
            return_minus_2 = pct_chg[-2]
        elif zero_crossings.size == 3:
            return_minus_5 = np.nan
            return_minus_4 = np.nan
            return_minus_3 = np.nan
            return_minus_2 = pct_chg[-2]
        elif zero_crossings.size == 2:
            return_minus_5 = np.nan
            return_minus_4 = np.nan
            return_minus_3 = np.nan
            return_minus_2 = np.nan
        else:
            return_minus_5 = np.nan
            return_minus_4 = np.nan
            return_minus_3 = np.nan
            return_minus_2 = np.nan

        X = pd.DataFrame(section_time)
        y = pd.DataFrame(section_price)
        lm = linear_model.LinearRegression()
        model_lm = lm.fit(X,y)
        lr_all_day_hat = np.array(lm.predict(X)).flatten()
        lr_all_day_time_passed = np.subtract(section_time[-1], section_time[0])
        lr_all_day_pct_chg = np.divide(np.subtract(lr_all_day_hat[-1], lr_all_day_hat[0]), lr_all_day_hat[0])
        lr_all_day_r2 = lm.score(X,y)

        if zero_crossings.size > 1:
            critical_points = np.array([])
            sec_pct_chg = np.array([])
            sec_curve_pct_chg = np.array([])
            curve_der_neg = section_der[int(zero_crossings[-2]):int(zero_crossings[-1]+1)]
            curve_price_hat_neg = section_price_hat[int(zero_crossings[-2]):int(zero_crossings[-1]+2)]
            curve_price_neg = section_price[int(zero_crossings[-2]):int(zero_crossings[-1]+2)]
            curve_time_neg = section_time[int(zero_crossings[-2]):int(zero_crossings[-1]+2)]
            critical_points = np.append(critical_points, 0)
            if curve_der_neg.size == 0:
                der_1 = np.array([])
                der_1 = np.append(der_1, np.zeros(8) + np.nan)
                der_2 = np.array([])
                der_2 = np.append(der_2, np.zeros(8) + np.nan)
                der_3 = np.array([])
                der_3 = np.append(der_3, np.zeros(8) + np.nan)
                sec_pct_chg = np.append(sec_pct_chg, np.zeros(8) + np.nan)
                sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(8) + np.nan)
                critical_points = np.append(critical_points, np.zeros(8) + np.nan)
            elif curve_der_neg.size > 0:
                inflect_pt_neg = int(np.nanargmin(curve_der_neg))
                curve_der_sec_1_neg = curve_der_neg[0:inflect_pt_neg+1]
                if curve_der_sec_1_neg.size == 0:
                    critical_points = np.append(critical_points, np.zeros(3) + np.nan)
                    critical_points = np.append(critical_points, inflect_pt_neg)
                elif curve_der_sec_1_neg.size > 0:
                    curve_der_415_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_1_neg, -.415)))
                    curve_der_entry_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_1_neg, -1)))
                    curve_der_241_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_1_neg, -2.41)))
                    critical_points = np.append(critical_points, curve_der_415_neg).astype(np.int64)
                    critical_points = np.append(critical_points, curve_der_entry_neg).astype(np.int64)
                    critical_points = np.append(critical_points, curve_der_241_neg).astype(np.int64)

                    
                if critical_points.size == 4:
                    critical_points = np.append(critical_points, inflect_pt_neg)
                elif critical_points.size == 1:
                    critical_points = np.append(critical_points, np.zeros(3) + np.nan)
                    critical_points = np.append(critical_points, inflect_pt_neg)

                curve_der_sec_2_neg = curve_der_neg[inflect_pt_neg:]
                if curve_der_sec_2_neg.size > 0:
                    curve_der_241_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_2_neg, -2.41)))
                    curve_der_entry_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_2_neg, -1)))
                    curve_der_415_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_2_neg, -.415)))
                    curve_der_241_neg = len(curve_der_sec_1_neg)-1 + curve_der_241_neg
                    curve_der_entry_neg = len(curve_der_sec_1_neg)-1 + curve_der_entry_neg
                    curve_der_415_neg = len(curve_der_sec_1_neg)-1 + curve_der_415_neg
                    critical_points = np.append(critical_points, curve_der_241_neg)
                    critical_points = np.append(critical_points, curve_der_entry_neg)
                    critical_points = np.append(critical_points, curve_der_415_neg)
                    critical_points = np.append(critical_points, len(curve_der_neg)-1)
                if critical_points.size == 5:
                    critical_points = np.append(critical_points, np.zeros(3) + np.nan)
                    critical_points = np.append(critical_points, len(curve_der_neg)-1)
                critical_points = critical_points.astype(np.int64)
                curve_der_neg = np.divide(np.diff(np.divide(np.diff(curve_price_hat_neg), curve_price_hat_neg[:-1])), np.diff(curve_time_neg[:-1]))
                


                @jit(parallel=True)
                def for_2(critical_points, curve_der_neg):
                    der_1 = np.array([])
                    for s in range(0, len(critical_points)-1):
                        if critical_points[s] <= -9223372036854775 or critical_points[s] == np.nan:
                            der_1 = np.append(der_1, np.nan)
                            continue
                        else:
                            if critical_points[s+1] <= -9223372036854775 or critical_points[s+1] == np.nan:
                                der_1 = np.append(der_1, np.nan)
                                continue
                            else:
                                
                                if critical_points[s+1]+1 < len(curve_der_neg):
                                    section_mean = np.nanmean(curve_der_neg[critical_points[s]:critical_points[s+1]+1])
                                    if section_mean.size == 0:
                                        section_mean = np.nan
                                    der_1 = np.append(der_1, section_mean)
                                else:
                                    section_mean = np.nanmean(curve_der_neg[critical_points[s]:critical_points[s+1]])
                                    if section_mean.size == 0:
                                        section_mean = np.nan
                                    der_1 = np.append(der_1, section_mean)
                    return der_1
                der_1 = for_2(critical_points, curve_der_neg)

                curve_der_neg_diff = np.diff(curve_der_neg)
                section_der_2_neg = np.divide(curve_der_neg_diff, np.diff(curve_time_neg[:-2]))

                @jit(parallel=True)
                def for_3(critical_points, section_der_2_neg):
                
                    der_2 = np.array([])
                    for s2 in range(0, len(critical_points)-1):
                        if critical_points[s2] <= -9223372036854775 or critical_points[s2] == np.nan:
                            der_2 = np.append(der_2, np.nan)
                            continue
                        else:
                            if critical_points[s2+1] <= -9223372036854775 or critical_points[s2+1] == np.nan:
                                der_2 = np.append(der_2, np.nan)
                                continue
                            else:
                                section_mean = np.nanmean(section_der_2_neg[critical_points[s2]:critical_points[s2+1]])
                                if section_mean.size == 0:
                                    section_mean = np.nan
                                der_2 = np.append(der_2, section_mean)
                    return der_2
                der_2 = for_3(critical_points, section_der_2_neg)

                section_der_2_neg_diff = np.diff(section_der_2_neg)
                section_der_3_neg = np.divide(section_der_2_neg_diff, np.diff(curve_time_neg[:-3]))
                
                @jit(parallel=True)
                def for_4(critical_points, section_der_3_neg):
                    der_3 = np.array([])
                    for s3 in range(0, len(critical_points)-1):
                        if critical_points[s3] <= -9223372036854775 or critical_points[s3] == np.nan:
                            der_3 = np.append(der_3, np.nan)
                            continue
                        else:
                            if critical_points[s3+1] <= -9223372036854775 or critical_points[s3+1] == np.nan:
                                der_3 = np.append(der_3, np.nan)
                                continue
                            else:
                                section_mean = np.nanmean(section_der_3_neg[critical_points[s3]:critical_points[s3+1]-1])
                                if section_mean.size == 0:
                                    section_mean = np.nan
                                der_3 = np.append(der_3, section_mean)
                    return der_3
                der_3 = for_4(critical_points, section_der_3_neg)

                @jit(parallel=True)
                def for_5(critical_points, curve_price_hat_neg):

                    sec_pct_chg = np.array([])
                    for s_p_c in range(0, len(critical_points)-1):
                        if critical_points[s_p_c] <= -9223372036854775 or critical_points[s_p_c] == np.nan:
                            sec_pct_chg = np.append(sec_pct_chg, np.nan)
                            continue
                        else:
                            if critical_points[s_p_c+1] <= -9223372036854775 or critical_points[s_p_c+1] == np.nan:
                                sec_pct_chg = np.append(sec_pct_chg, np.nan)
                                continue
                            else:
                                if critical_points[s_p_c+1]+1 < len(curve_price_hat_neg):
                                    section_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_p_c+1]+1], curve_price_hat_neg[critical_points[s_p_c]]), curve_price_hat_neg[critical_points[s_p_c]])
                                    if section_pct_chg.size == 0:
                                        section_pct_chg = np.nan
                                    sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
                                else:
                                    section_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_p_c+1]], curve_price_hat_neg[critical_points[s_p_c]]), curve_price_hat_neg[critical_points[s_p_c]])
                                    if section_pct_chg.size == 0:
                                        section_pct_chg = np.nan
                                    sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
                    return sec_pct_chg
                sec_pct_chg = for_5(critical_points, curve_price_hat_neg)

                @jit(parallel=True)
                def for_6(critical_points, curve_price_hat_neg):
                    sec_curve_pct_chg = np.array([])
                    for s_c_p_c in range(0, len(critical_points)-1):
                        if critical_points[s_c_p_c] <= -9223372036854775 or critical_points[s_c_p_c] == np.nan:
                            sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.nan)
                            continue
                        else:
                            if critical_points[s_c_p_c+1] <= -9223372036854775 or critical_points[s_c_p_c+1] == np.nan:
                                sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.nan)
                                continue
                            else:
                                if critical_points[s_c_p_c+1]+1 < len(curve_price_hat_neg):
                                    section_curve_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_c_p_c+1]+1], curve_price_hat_neg[critical_points[0]]), curve_price_hat_neg[critical_points[0]])
                                    if section_curve_pct_chg.size == 0:
                                        section_curve_pct_chg = np.nan
                                    sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
                                else:
                                    section_curve_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_c_p_c+1]], curve_price_hat_neg[critical_points[0]]), curve_price_hat_neg[critical_points[0]])
                                    if section_curve_pct_chg.size == 0:
                                        section_curve_pct_chg = np.nan
                                    sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
                    return sec_curve_pct_chg
                sec_curve_pct_chg = for_6(critical_points, curve_price_hat_neg)




            curve_der = section_der[int(zero_crossings[-1]):]
            if curve_der.size == 0:
                der_1 = np.append(der_1, np.zeros(2) + np.nan)
                der_2 = np.append(der_2, np.zeros(2) + np.nan)
                der_3 = np.append(der_3, np.zeros(2) + np.nan)
                sec_pct_chg = np.append(sec_pct_chg, np.zeros(2) + np.nan)
                sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(2) + np.nan)
                critical_points = np.append(critical_points, np.zeros(2) + np.nan)
            elif curve_der.size > 0:
                curve_der_sec_1 = curve_der
                curve_der_entry = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, 1)))
                curve_der_415 = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, .415)))
                if critical_points.size == 8:
                    critical_points = np.append(critical_points, 0).astype(np.int64)
                    critical_points = np.append(critical_points, curve_der_415).astype(np.int64)
                    critical_points = np.append(critical_points, curve_der_entry).astype(np.int64)
                elif critical_points.size == 9:
                    critical_point_10 = len(curve_der_neg)-1 + curve_der_415
                    critical_point_11 = len(curve_der_neg)-1 + curve_der_entry
                    critical_points = np.append(critical_points, critical_point_10).astype(np.int64)
                    critical_points = np.append(critical_points, critical_point_11).astype(np.int64)
                
                critical_points = critical_points.astype(np.int64)

                curve_price = section_price[int(zero_crossings[-1]):int(zero_crossings[-2])+int(critical_points[-1])+2]
                curve_time = section_time[int(zero_crossings[-1]):int(zero_crossings[-2])+int(critical_points[-1])+2]
                curve_price_hat = section_price_hat[int(zero_crossings[-1]):int(zero_crossings[-2])+int(critical_points[-1])+2]
                curve_price_hat_diff_div_diff = np.diff(np.divide(np.diff(curve_price_hat), curve_price_hat[:-1])) 			
                curve_der = np.divide(curve_price_hat_diff_div_diff, np.diff(curve_time)[:-1])
                curve_time_r = section_time[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
                curve_price_r = section_price[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
                curve_price_hat_r = section_price_hat[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
                curve_vol_r = section_vol[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]

                curve_der_diff = np.diff(curve_der)
                section_der_2 = np.divide(curve_der_diff, np.diff(curve_time[:-2]))
                


                critical_points_pos = np.array([0, critical_points[-2]-(len(curve_der_neg)-1), critical_points[-1]-(len(curve_der_neg)-1)]).astype(np.int64)
                @jit(parallel=True)
                def for_7(critical_points_pos, curve_der, der_1):
                    for p in range(0, len(critical_points_pos)-1):
                        if critical_points_pos[p+1]+1 < len(curve_der):
                            section_mean = np.nanmean(curve_der[critical_points_pos[p]:critical_points_pos[p+1]+1])
                            if section_mean.size == 0:
                                section_mean = np.nan
                            der_1 = np.append(der_1, section_mean)
                        else:
                            section_mean = np.nanmean(curve_der[critical_points_pos[p]:critical_points_pos[p+1]])
                            if section_mean.size == 0:
                                section_mean = np.nan
                            der_1 = np.append(der_1, section_mean)
                    return der_1
                der_1 = for_7(critical_points_pos, curve_der, der_1)
                        
                @jit(parallel=True)
                def for_8(critical_points_pos, section_der_2, der_2):
                    for p2 in range(0, len(critical_points_pos)-1):
                        section_mean = np.nanmean(section_der_2[critical_points_pos[p2]:critical_points_pos[p2+1]])
                        if section_mean == 0:
                            section_mean = np.nan
                        der_2 = np.append(der_2, section_mean)
                    return der_2

                der_2 = for_8(critical_points_pos, section_der_2, der_2)

                section_der_2_diff = np.diff(section_der_2)
                section_der_3 = np.divide(section_der_2_diff, np.diff(curve_time[:-3]))			
                
                @jit(parallel=True)
                def for_9(critical_points_pos, section_der_3, der_3):
                    for p3 in range(0, len(critical_points_pos)-1):
                        section_mean = np.nanmean(section_der_3[critical_points_pos[p3]:critical_points_pos[p3+1]-1])
                        if section_mean == 0:
                            section_mean = np.nan
                        der_3 = np.append(der_3, np.nan)
                    return der_3
                der_3 = for_9(critical_points_pos, section_der_3, der_3)

                @jit(parallel=True)
                def for_10(critical_points_pos, curve_price_hat, sec_pct_chg):
                    for s_p_c_2 in range(0, len(critical_points_pos)-1):
                        if critical_points_pos[s_p_c_2+1]+1 < len(curve_price_hat):
                            section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2+1]+1], curve_price_hat[critical_points_pos[s_p_c_2]]), curve_price_hat[critical_points_pos[s_p_c_2]])
                            if section_pct_chg.size == 0:
                                section_pct_chg = np.nan
                            sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
                        else:
                            section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2+1]], curve_price_hat[critical_points_pos[s_p_c_2]]), curve_price_hat[critical_points_pos[s_p_c_2]])
                            if section_pct_chg.size == 0:
                                section_pct_chg = np.nan
                            sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
                    return sec_pct_chg

                sec_pct_chg = for_10(critical_points_pos, curve_price_hat, sec_pct_chg)

                @jit(parallel=True)
                def for_11(critical_points_pos, curve_price_hat, sec_curve_pct_chg):
                    for s_c_p_c_2 in range(0, len(critical_points_pos)-1):
                        if critical_points_pos[s_p_c_2+1]+1 < len(curve_price_hat):
                            section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2+1]+1], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
                            if section_curve_pct_chg.size == 0:
                                section_curve_pct_chg = np.nan
                            sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
                        else:
                            section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2+1]], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
                            if section_curve_pct_chg.size == 0:
                                section_curve_pct_chg = np.nan
                            sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
                    return sec_curve_pct_chg

                sec_curve_pct_chg = for_11(critical_points_pos, curve_price_hat, sec_curve_pct_chg)

            r_sq = np.array([])
            std_per_section = np.array([])
            residual_max_per_section = np.array([])
            residual_mean_per_section = np.array([])
            if curve_price_hat_r.size == 0:
                r_sq = np.append(r_sq, np.zeros(10) + np.nan)
                std_per_section = np.append(std_per_section, np.zeros(10) + np.nan)
                residual_max_per_section = np.append(residual_max_per_section, np.zeros(10) + np.nan)
                residual_mean_per_section = np.append(residual_mean_per_section, np.zeros(10) + np.nan)
            elif curve_price_hat_r.size > 0:
                @jit(parallel=True)
                def for_12(critical_points,r_sq,std_per_section,residual_max_per_section,residual_mean_per_section,curve_time_r,curve_price_r,curve_price_hat_r):
                    for r in range(0, len(critical_points)-1):
                        if critical_points[r] <= -9223372036854775 or critical_points[r] == np.nan:
                            r_sq = np.append(r_sq, np.nan)
                            std_per_section = np.append(std_per_section, np.nan)
                            residual_max_per_section = np.append(residual_max_per_section, np.nan)
                            residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
                            continue
                        else:
                            if critical_points[r+1] <= -9223372036854775 or critical_points[r+1] == np.nan:
                                r_sq = np.append(r_sq, np.nan)
                                std_per_section = np.append(std_per_section, np.nan)
                                residual_max_per_section = np.append(residual_max_per_section, np.nan)
                                residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
                                continue
                            else:
                                
                                if critical_points[r+1] == critical_points[r]:
                                    if r_sq.size == 0 or std_per_section.size == 0 or residual_max_per_section.size == 0 or residual_mean_per_section.size == 0 :
                                        r_sq = np.append(r_sq, np.nan)
                                        std_per_section = np.append(std_per_section, np.nan)
                                        residual_max_per_section = np.append(residual_max_per_section, np.nan)
                                        residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
                                    else:	
                                        r += 1
                                        r_sq = np.append(r_sq, r_sq[-1])
                                        std_per_section = np.append(std_per_section, std_per_section[-1])
                                        residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section[-1])
                                        residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section[-1])
                                        continue
                                else:
                                    r_sq_2 = model_pyearth.score(curve_time_r[critical_points[r]:critical_points[r+1]+2], curve_price_r[critical_points[r]:critical_points[r+1]+2])
                                    r_sq = np.append(r_sq, r_sq_2)
                                    avg_mean = np.nanmean(curve_price_r[critical_points[r]:critical_points[r+1]+2])
                                    std_sec = np.nanstd(curve_price_r[critical_points[r]:critical_points[r+1]+2])
                                    std_per_section = np.append(std_per_section, std_sec)
                                    curvy = curve_price_hat_r[critical_points[r]:critical_points[r+1]+2]
                                    residual_max_per_section_0_0_0 = np.abs(np.subtract(curve_price_r[critical_points[r]:critical_points[r+1]+2], curve_price_hat_r[critical_points[r]:critical_points[r+1]+2]))
                                    residual_max_per_section_0_0 = np.nanargmax(residual_max_per_section_0_0_0)
                                    residual_max_per_section_0 = np.take(residual_max_per_section_0_0_0, residual_max_per_section_0_0)
                                    residual_mean_per_section_0 = np.nanmean(np.abs(np.subtract(curve_price_r[critical_points[r]:critical_points[r+1]+2], curve_price_hat_r[critical_points[r]:critical_points[r+1]+2])))
                                    residual_max_per_section_0_ph = np.divide(residual_max_per_section_0, np.take(curvy, residual_max_per_section_0_0))
                                    residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section_0_ph)
                                    residual_mean_per_section_0_ph = np.nanmean(curvy)
                                    residual_mean_per_section_div = np.divide(residual_mean_per_section_0, residual_mean_per_section_0_ph)
                                    residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section_div)
                    return np.array([r_sq,std_per_section,residual_max_per_section,residual_mean_per_section])

                for_12_variables = for_12(critical_points,r_sq,std_per_section,residual_max_per_section,residual_mean_per_section,curve_time_r,curve_price_r,curve_price_hat_r)
                r_sq = for_12_variables[0]
                std_per_section = for_12_variables[1]
                residual_max_per_section = for_12_variables[2]
                residual_mean_per_section = for_12_variables[3]
            
            r_sq = r_sq.astype(np.float64)

            time_since_maxima = np.array([])
            time_since_last_sect = np.array([])
            if curve_price_hat_r.size == 0:
                time_since_maxima = np.append(time_since_maxima, np.zeros(10) + np.nan)
                time_since_last_sect = np.append(time_since_last_sect, np.zeros(10) + np.nan)
            elif curve_price_hat_r.size > 0:
                @jit(parallel=True)
                def for_13(critical_points,time_since_maxima,time_since_last_sect,curve_time_r):
                    for c in range(0, len(critical_points)-1):
                        if critical_points[c] <= -9223372036854775 or critical_points[c] == np.nan:
                            time_since_maxima = np.append(time_since_maxima, np.nan)
                            time_since_last_sect = np.append(time_since_last_sect, np.nan)
                            continue
                        else:
                            if critical_points[c+1] <= -9223372036854775 or critical_points[c+1] == np.nan:
                                time_since_maxima = np.append(time_since_maxima, np.nan)
                                time_since_last_sect = np.append(time_since_last_sect, np.nan)
                                continue
                            else:
                                if critical_points[c+1] == critical_points[c]:
                                    c += 1
                                    time_since_maxima = np.append(time_since_maxima, np.zeros(1))
                                    time_since_last_sect = np.append(time_since_last_sect, np.zeros(1))
                                    continue
                                else:
                                    if critical_points[0] <= -9223372036854775 or critical_points[0] == np.nan:
                                        time_since_maxima_append = np.nan
                                    else:
                                        if critical_points[c+1]+1 < len(curve_time_r)-1:
                                            time_since_maxima_append = curve_time_r[critical_points[c+1]+1] - curve_time_r[critical_points[0]]
                                            if time_since_maxima_append.size == 0:
                                                time_since_maxima_append = np.nan
                                            time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
                                            time_since_last_sect_append = curve_time_r[critical_points[c+1]+1] - curve_time_r[critical_points[c]]
                                            time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)                 
                                        else:
                                            time_since_maxima_append = curve_time_r[critical_points[c+1]] - curve_time_r[critical_points[0]]
                                            if time_since_maxima_append.size == 0:
                                                time_since_maxima_append = np.nan
                                            time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
                                            time_since_last_sect_append = curve_time_r[critical_points[c+1]] - curve_time_r[critical_points[c]]
                                            time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)
                    return np.array([time_since_maxima,time_since_last_sect])

                for_13_variables = for_13(critical_points,time_since_maxima,time_since_last_sect,curve_time_r)
                time_since_maxima = for_13_variables[0]
                time_since_last_sect = for_13_variables[1]
            

            #residual_max_0_0 = np.abs(np.subtract(curve_price_r, curve_price_hat_r))
            #residual_max_0 = np.nanargmax(residual_max_0_0)
            #residual_max = np.take(residual_max_0_0, residual_max_0)
            #residual_mean = np.nanmean(np.abs(np.subtract(curve_price_r, curve_price_hat_r)))

            residual_max_2 = np.abs(np.subtract(curve_price_r, curve_price_hat_r))
            residual_max_1 = np.nanargmax(residual_max_2)
            residual_max_0 = np.take(residual_max_2, residual_max_1)
            residual_max_0_ph = np.take(curve_price_hat_r, residual_max_1)
            residual_max = np.divide(residual_max_0, residual_max_0_ph)
            residual_mean_0 = np.nanmean(np.abs(np.subtract(curve_price_r, curve_price_hat_r)))
            residual_mean = np.divide(residual_mean_0, np.nanmean(curve_price_hat_r))


            if critical_points.size > 0 and curve_price_r.size > 0:
                current_price = curve_price_r[-1]
            else:
                current_price = np.nan

            if critical_points.size > 0 and curve_price_hat_r.size > 0:
                current_price_hat_r = curve_price_hat_r[-1]
            else:
                current_price_hat_r = np.nan

            if curve_price_r.size > 0:
                avg_Price = np.nanmean(curve_price_r)
            else:
                avg_Price = np.nan
            
            current_datetime = np.multiply(curve_time[-1],1000000000).astype(np.int64).item()
            current_date = datetime(1970, 1, 1) + timedelta(seconds=current_datetime)
            current_date = current_date.strftime('%Y-%m-%d')
            open_ts = datetime.strptime(current_date + ' 13:30:00', '%Y-%m-%d %H:%M:%S')
            dt = pytz.timezone('America/Chicago').normalize(open_ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Chicago')))
            is_dst = bool(dt.dst())
            if is_dst is True:
                offset_open = int(time.mktime(time.strptime(current_date + ' 13:30:00', '%Y-%m-%d %H:%M:%S')))
            elif is_dst is False:
                offset_open = int(time.mktime(time.strptime(current_date + ' 14:30:00', '%Y-%m-%d %H:%M:%S')))
            if critical_points.size > 0 and curve_time_r.size > 0:
                current_unix_time = np.multiply(curve_time_r[-1],1000000000)
                ms_since_open = np.multiply(curve_time_r[-1],1000000000) - offset_open
            else:
                current_unix_time = np.nan
                ms_since_open = np.nan
            
            current_date = datetime.strptime(current_date, '%Y-%m-%d')

            

            first_day = current_date.replace(day=1)
            dom = current_date.day
            adjusted_dom = dom + first_day.weekday()

            

            total_days = int((current_date - datetime(1970,1,1)).days)



            

            price_pcavg_per_section = np.array([])
            if curve_price_hat_r.size == 0:
                price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(10) + np.nan)
            elif curve_price_hat_r.size > 0:
                @jit(parallel=True)
                def for_14(critical_points,price_pcavg_per_section,curve_price_hat_r):
                    for avg_p_n in range(0, len(critical_points)-1):
                        if critical_points[avg_p_n] <= -9223372036854775 or critical_points[avg_p_n] == np.nan:
                            price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
                            continue
                        else:
                            if critical_points[avg_p_n+1] <= -9223372036854775 or critical_points[avg_p_n+1] == np.nan:
                                price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
                                continue
                            else:
                                if critical_points[avg_p_n+1] == critical_points[avg_p_n]:
                                    avg_p_n += 1
                                    price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(1))
                                    continue
                                else:
                                    price_pcavg_per_section = np.append(price_pcavg_per_section, np.nanmean(np.diff(curve_price_hat_r[critical_points[avg_p_n]:critical_points[avg_p_n+1]+2])/curve_price_hat_r[critical_points[avg_p_n]:critical_points[avg_p_n+1]+2][:-1]))
                    return price_pcavg_per_section
            
                price_pcavg_per_section = for_14(critical_points,price_pcavg_per_section,curve_price_hat_r)


            price_pct_chg_neg = np.divide(np.subtract(curve_price_neg[-1], curve_price_neg[0]), curve_price_neg[0])
            price_pct_chg_pos = np.divide(np.subtract(curve_price[-1], curve_price[0]), curve_price[0])





            lm = linear_model.LinearRegression()





            X = pd.DataFrame(curve_time_neg)
            y = pd.DataFrame(curve_price_neg)
            y =  Imputer().fit_transform(y)
            model_lm = lm.fit(X,y)
            lr_curve_neg_hat = np.array(lm.predict(X)).flatten()
            lr_price_total_pct_chg_neg = np.divide(np.subtract(lr_curve_neg_hat[-1], lr_curve_neg_hat[0]), lr_curve_neg_hat[0])
            lr_price_avg_roc_neg = np.nanmean((np.diff(lr_curve_neg_hat)/lr_curve_neg_hat[:-1])/np.diff(curve_time_neg))
            lr_price_r2_neg = lm.score(X,y)




            X = pd.DataFrame(curve_time)
            y = pd.DataFrame(curve_price)
            y =  Imputer().fit_transform(y)
            model_lm = lm.fit(X,y)
            lr_curve_pos_hat = np.array(lm.predict(X)).flatten()
            lr_price_total_pct_chg_pos = np.divide(np.subtract(lr_curve_pos_hat[-1], lr_curve_pos_hat[0]), lr_curve_pos_hat[0])
            lr_price_avg_roc_pos = np.nanmean((np.diff(lr_curve_pos_hat)/lr_curve_pos_hat[:-1])/np.diff(curve_time))
            lr_price_r2_pos = lm.score(X,y)






            lr_price_pct_chg_per_section = np.array([])
            lr_price_roc_per_section = np.array([])
            lr_price_r2_per_section = np.array([])
            if curve_price_r.size == 0:
                lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(10) + np.nan)
                lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(10) + np.nan)
                lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(10) + np.nan)
            elif curve_price_r.size > 0:
                @jit(parallel=True)
                def for_15(critical_points,lr_price_pct_chg_per_section,lr_price_roc_per_section,lr_price_r2_per_section,curve_time_r,curve_price_r):                
                    for avg_p in range(0, len(critical_points)-1):
                        if critical_points[avg_p] <= -9223372036854775 or critical_points[avg_p] == np.nan:
                            lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
                            lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
                            lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
                            continue
                        else:
                            if critical_points[avg_p+1] <= -9223372036854775 or critical_points[avg_p+1] == np.nan:
                                lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
                                lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
                                lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
                                continue
                            else:
                                if critical_points[avg_p+1] == critical_points[avg_p]:
                                    avg_p += 1
                                    lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(1))
                                    lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(1))
                                    lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(1))
                                    continue
                                else:
                                    X = pd.DataFrame(curve_time_r[critical_points[avg_p]:critical_points[avg_p+1]+2])
                                    y = pd.DataFrame(curve_price_r[critical_points[avg_p]:critical_points[avg_p+1]+2])
                                    y =  Imputer().fit_transform(y)
                                    model_lm = lm.fit(X,y)
                                    lr_price_per_section_hat = np.array(lm.predict(X)).flatten()
                                    lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.divide(np.subtract(lr_price_per_section_hat[-1], lr_price_per_section_hat[0]), lr_price_per_section_hat[0]))
                                    lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nanmean((np.diff(lr_price_per_section_hat)/lr_price_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[avg_p]:critical_points[avg_p+1]+2])))
                                    lr_price_r2_per_section_2 = lm.score(X,y)
                                    lr_price_r2_per_section = np.append(lr_price_r2_per_section, lr_price_r2_per_section_2)
                    return np.array([lr_price_pct_chg_per_section,lr_price_roc_per_section,lr_price_r2_per_section])

                for_15_variables = for_15(critical_points,lr_price_pct_chg_per_section,lr_price_roc_per_section,lr_price_r2_per_section,curve_time_r,curve_price_r)
                lr_price_pct_chg_per_section = for_15_variables[0]
                lr_price_roc_per_section = for_15_variables[1]
                lr_price_r2_per_section = for_15_variables[2]

            section_time_key_neg = curve_time_neg
            section_vol_key_neg = section_vol[int(zero_crossings[-2]):int(zero_crossings[-1])+2]

            X = pd.DataFrame(section_time_key_neg)
            y = pd.DataFrame(section_vol_key_neg)
            y =  Imputer().fit_transform(y)
            model_lm = lm.fit(X,y)
            section_vol_neg_hat = np.array(lm.predict(X)).flatten()
            vol_total_pct_chg_neg = np.divide(np.subtract(section_vol_neg_hat[-1], section_vol_neg_hat[0]), section_vol_neg_hat[0])
            vol_r2_neg = lm.score(X,y)




            section_time_key_pos = curve_time_r[int(critical_points[-3]):int(critical_points[-1]+2)]
            section_vol_key_pos = curve_vol_r[int(critical_points[-3]):int(critical_points[-1]+2)]

            X = pd.DataFrame(section_time_key_pos)
            y = pd.DataFrame(section_vol_key_pos)
            y =  Imputer().fit_transform(y)
            model_lm = lm.fit(X,y)
            section_vol_pos_hat = np.array(lm.predict(X)).flatten()
            vol_total_pct_chg_pos = np.divide(np.subtract(section_vol_pos_hat[-1], section_vol_pos_hat[0]), section_vol_pos_hat[0])
            vol_r2_pos = lm.score(X,y)




            section_mf_key_total = np.multiply(curve_price_r, curve_vol_r)

            section_time_key_neg = curve_time_neg
            section_mf_price_key_neg = np.multiply(curve_price_neg, section_vol_key_neg)

            X = pd.DataFrame(section_time_key_neg)
            y = pd.DataFrame(section_mf_price_key_neg)
            y =  Imputer().fit_transform(y)
            model_lm = lm.fit(X,y)
            section_mf_price_neg_hat = np.array(lm.predict(X)).flatten()
            mf_price_r2_neg = lm.score(X,y)




            section_time_key_pos = curve_time
            section_mf_price_key_pos = np.multiply(curve_price, section_vol_key_pos)

            X = pd.DataFrame(section_time_key_pos)
            y = pd.DataFrame(section_mf_price_key_pos)
            y =  Imputer().fit_transform(y)
            model_lm = lm.fit(X,y)
            section_mf_price_pos_hat = np.array(lm.predict(X)).flatten()
            mf_price_r2_pos = lm.score(X,y)



            avg_vol = np.nanmean(curve_vol_r)

            

            @jit(parallel=True)
            def for_16(critical_points,curve_time_r):                
                quote_key_times_append = np.array([])
                quote_key_times = np.array([])
                for cp in range(0, len(critical_points)):
                    if critical_points[cp] <= -9223372036854775 or critical_points[cp] == np.nan:
                        quote_key_times = np.append(quote_key_times, np.nan)
                        continue
                    else:
                        if critical_points[cp]+1 < len(curve_time_r) and cp == 0:
                            quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp])], 1000000000)
                            if quote_key_times_append.size == 0:
                                quote_key_times_append = np.nan
                            quote_key_times = np.append(quote_key_times, quote_key_times_append)
                        elif critical_points[cp]+1 < len(curve_time_r) and cp > 0:
                            quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp]+1)], 1000000000)
                            if quote_key_times_append.size == 0:
                                quote_key_times_append = np.nan
                            quote_key_times = np.append(quote_key_times, quote_key_times_append)
                        elif critical_points[cp]+1 >= len(curve_time_r):
                            quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp])], 1000000000)
                            if quote_key_times_append.size == 0:
                                quote_key_times_append = np.nan
                            quote_key_times = np.append(quote_key_times, quote_key_times_append)
                return quote_key_times
            quote_key_times = for_16(critical_points,curve_time_r)



            
            final_output.update(dict(symbol = symbol))
            final_output.update(dict(avg_return_40_mins = avg_return_40_mins)) 
            final_output.update(dict(avg_chg_in_avg_return = avg_chg_in_avg_return)) 
            final_output.update(dict(return_minus_5 = return_minus_5)) 
            final_output.update(dict(return_minus_4 = return_minus_4)) 
            final_output.update(dict(return_minus_3 = return_minus_3)) 
            final_output.update(dict(return_minus_2 = return_minus_2)) 
            final_output.update(dict(lr_all_day_time_passed = lr_all_day_time_passed)) 
            final_output.update(dict(lr_all_day_pct_chg = lr_all_day_pct_chg))
            final_output.update(dict(lr_all_day_r2 = lr_all_day_r2)) 
            final_output.update(dict(zip(['sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1'], der_1.T)))
            final_output.update(dict(zip(['sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2'], der_2.T)))
            final_output.update(dict(zip(['sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3'], der_3.T)))
            final_output.update(dict(zip(['sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq'], r_sq.T)))   
            final_output.update(dict(zip(['sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section'], std_per_section.T)))   
            final_output.update(dict(zip(['sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section'], residual_max_per_section.T)))   
            final_output.update(dict(zip(['sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section'], residual_mean_per_section.T)))   
            final_output.update(dict(zip(['sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg'], sec_pct_chg.T)))
            final_output.update(dict(zip(['sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg'], sec_curve_pct_chg.T)))
            final_output.update(dict(zip(['sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima'], time_since_maxima.T)))
            final_output.update(dict(zip(['sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect'], time_since_last_sect.T)))
            final_output.update(dict(sec_1_residual_max = residual_max))
            final_output.update(dict(sec_1_residual_mean = residual_mean))				
            final_output.update(dict(sec_1_current_unix_time = current_unix_time)) 
            final_output.update(dict(sec_1_ms_since_open = ms_since_open)) 
            final_output.update(dict(total_days = total_days)) 
            final_output.update(dict(sec_1_current_price = current_price)) 
            final_output.update(dict(sec_1_current_price_hat_r = current_price_hat_r)) 				
            final_output.update(dict(sec_1_avg_price = avg_Price)) 
            final_output.update(dict(zip(['sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section'], price_pcavg_per_section.T))) 
            final_output.update(dict(zip(['sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section'], lr_price_pct_chg_per_section.T))) 
            final_output.update(dict(zip(['sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section'], lr_price_roc_per_section.T))) 
            final_output.update(dict(zip(['sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section'], lr_price_r2_per_section.T)))
            final_output.update(dict(sec_1_price_pct_chg_neg = price_pct_chg_neg)) 
            final_output.update(dict(sec_1_price_pct_chg_pos = price_pct_chg_pos)) 
            final_output.update(dict(sec_1_lr_price_total_pct_chg_neg = lr_price_total_pct_chg_neg)) 
            final_output.update(dict(sec_1_lr_price_avg_roc_neg = lr_price_avg_roc_neg)) 
            final_output.update(dict(sec_1_lr_price_r2_neg = lr_price_r2_neg)) 
            final_output.update(dict(sec_1_lr_price_total_pct_chg_pos = lr_price_total_pct_chg_pos)) 
            final_output.update(dict(sec_1_lr_price_avg_roc_pos = lr_price_avg_roc_pos)) 
            final_output.update(dict(sec_1_lr_price_r2_pos = lr_price_r2_pos)) 
            final_output.update(dict(sec_1_vol_total_pct_chg_neg = vol_total_pct_chg_neg)) 
            final_output.update(dict(sec_1_vol_total_pct_chg_pos = vol_total_pct_chg_pos)) 
            final_output.update(dict(sec_1_vol_r2_neg = vol_r2_neg)) 
            final_output.update(dict(sec_1_vol_r2_pos = vol_r2_pos)) 
            final_output.update(dict(sec_1_mf_price_r2_neg = mf_price_r2_neg)) 
            final_output.update(dict(sec_1_mf_price_r2_pos = mf_price_r2_pos)) 
            final_output.update(dict(sec_1_avg_vol = avg_vol)) 








            start_idx_price = price_pd.index[0]
            end_idx_price = price_pd.index[-1]
            quote_key = pd.DataFrame({'A' : []})
            #for quotes_side in quotes_side:
            if quotes_side.empty is False:
                #if quotes_side['Symbol'][0] == price_pd['Symbol'][0] and quotes_side.index[0].date() == price_pd.index[0].date():
                start_idx_quote = quotes_side.index.get_loc(start_idx_price, method='bfill')
                #start_idx_quote = (quotes_side.index-start_idx_price).abs().idxmin()
                end_idx_quote = quotes_side.index.get_loc(end_idx_price, method='ffill')
                #end_idx_quote = (quotes_side.index-end_idx_price).abs().idxmin()
                quote_key = quotes_side.iloc[start_idx_quote:end_idx_quote+1,:]
            if quote_key.empty is True:
                avg_askPrice = np.nan
                avg_bidPrice = np.nan 
                current_askSize = np.nan
                current_bidSize = np.nan                           
                avg_askSize = np.nan                          
                avg_bidSize = np.nan               
                mf_ask_total_pct_chg_neg = np.nan                                                                         
                mf_ask_avg_roc_neg = np.nan                                                                        
                mf_ask_r2_neg = np.nan                                                                         
                mf_bid_total_pct_chg_neg = np.nan                                                                         
                mf_bid_avg_roc_neg = np.nan                                                                         
                mf_bid_r2_neg = np.nan                                                                         
                ba_spread_total_pct_chg_neg = np.nan                                                                         
                ba_spread_r2_neg = np.nan                                                                                                                                             
                askPrice_total_pct_chg_neg = np.nan                                                                         
                askPrice_avg_roc_neg = np.nan                                                                        
                askPrice_r2_neg = np.nan                                                                         
                bidPrice_total_pct_chg_neg = np.nan                                                                         
                bidPrice_avg_roc_neg = np.nan                                                                         
                bidPrice_r2_neg = np.nan                                                              
                mf_ask_total_pct_chg_pos = np.nan                                     
                mf_ask_avg_roc_pos = np.nan                                     
                mf_ask_r2_pos = np.nan                                     
                mf_bid_total_pct_chg_pos = np.nan                                     
                mf_bid_avg_roc_pos = np.nan                                                                         
                mf_bid_r2_pos = np.nan                                                                         
                ba_spread_total_pct_chg_pos = np.nan                                                                         
                ba_spread_r2_pos = np.nan                                                                   
                askPrice_total_pct_chg_pos = np.nan                                                                      
                askPrice_avg_roc_pos = np.nan                                                                       
                askPrice_r2_pos = np.nan                                   
                bidPrice_total_pct_chg_pos = np.nan                                                                        
                bidPrice_avg_roc_pos = np.nan                                                                        
                bidPrice_r2_pos = np.nan


                final_output.update(dict(sec_1_avg_askPrice = avg_askPrice))                              
                final_output.update(dict(sec_1_avg_bidPrice = avg_bidPrice)) 
                final_output.update(dict(sec_1_current_askSize = current_askSize))                              
                final_output.update(dict(sec_1_current_bidSize = current_bidSize))   
                final_output.update(dict(sec_1_avg_askSize = avg_askSize))                              
                final_output.update(dict(sec_1_avg_bidSize = avg_bidSize))                                 
                final_output.update(dict(sec_1_mf_ask_total_pct_chg_neg = mf_ask_total_pct_chg_neg))                                                                         
                final_output.update(dict(sec_1_mf_ask_avg_roc_neg = mf_ask_avg_roc_neg))                                                                         
                final_output.update(dict(sec_1_mf_ask_r2_neg = mf_ask_r2_neg))                                                                         
                final_output.update(dict(sec_1_mf_bid_total_pct_chg_neg = mf_bid_total_pct_chg_neg))                                                                         
                final_output.update(dict(sec_1_mf_bid_avg_roc_neg = mf_bid_avg_roc_neg))                                                                         
                final_output.update(dict(sec_1_mf_bid_r2_neg = mf_bid_r2_neg))                                                                         
                final_output.update(dict(sec_1_ba_spread_total_pct_chg_neg = ba_spread_total_pct_chg_neg))                                                                         
                final_output.update(dict(sec_1_ba_spread_r2_neg = ba_spread_r2_neg))                                                                         
                final_output.update(dict(sec_1_askPrice_total_pct_chg_neg = askPrice_total_pct_chg_neg))                                                                         
                final_output.update(dict(sec_1_askPrice_avg_roc_neg = askPrice_avg_roc_neg))                                                                        
                final_output.update(dict(sec_1_askPrice_r2_neg = askPrice_r2_neg))                                                                         
                final_output.update(dict(sec_1_bidPrice_total_pct_chg_neg = bidPrice_total_pct_chg_neg))                                                                         
                final_output.update(dict(sec_1_bidPrice_avg_roc_neg = bidPrice_avg_roc_neg))                                                                         
                final_output.update(dict(sec_1_bidPrice_r2_neg = bidPrice_r2_neg))                                                                 
                final_output.update(dict(sec_1_mf_ask_total_pct_chg_pos = mf_ask_total_pct_chg_pos))                                     
                final_output.update(dict(sec_1_mf_ask_avg_roc_pos = mf_ask_avg_roc_pos))                                     
                final_output.update(dict(sec_1_mf_ask_r2_pos = mf_ask_r2_pos))                                     
                final_output.update(dict(sec_1_mf_bid_total_pct_chg_pos = mf_bid_total_pct_chg_pos))                                     
                final_output.update(dict(sec_1_mf_bid_avg_roc_pos = mf_bid_avg_roc_pos))                                                                         
                final_output.update(dict(sec_1_mf_bid_r2_pos = mf_bid_r2_pos))                                                                         
                final_output.update(dict(sec_1_ba_spread_total_pct_chg_pos = ba_spread_total_pct_chg_pos))                                                                         
                final_output.update(dict(sec_1_ba_spread_r2_pos = ba_spread_r2_pos))                                                                   
                final_output.update(dict(sec_1_askPrice_total_pct_chg_pos = askPrice_total_pct_chg_pos))                                                                     
                final_output.update(dict(sec_1_askPrice_avg_roc_pos = askPrice_avg_roc_pos))                                                                      
                final_output.update(dict(sec_1_askPrice_r2_pos = askPrice_r2_pos))                                  
                final_output.update(dict(sec_1_bidPrice_total_pct_chg_pos = bidPrice_total_pct_chg_pos))                                                                       
                final_output.update(dict(sec_1_bidPrice_avg_roc_pos = bidPrice_avg_roc_pos))                                                                       
                final_output.update(dict(sec_1_bidPrice_r2_pos = bidPrice_r2_pos))  
                final_output.update(dict(ts = None))
                final_output.update(dict(final_y = None))

            elif quote_key.empty is False:
                key_total = quote_key[(quote_key.index >= datetime.fromtimestamp(quote_key_times[0], pytz.utc)) & (quote_key.index <= datetime.fromtimestamp(quote_key_times[-1], pytz.utc))]
                if key_total.empty is False:
                    @jit(parallel=True)
                    def for_17(key_total,quote_key_times):                
                        section_time_key_total = key_total.index.values.astype(int)
                        section_time_key_total = np.divide(section_time_key_total, 1000)
                        quotes_critical_points = np.array([])
                        for qkt in range(0, len(quote_key_times)):
                            if quote_key_times[qkt] <= -9223372036854775 or quote_key_times[qkt] == np.nan:
                                quotes_critical_points = np.append(quotes_critical_points, np.nan)
                                continue
                            
                            else:
                                if qkt < len(quote_key_times)-1:
                                    quotes_critical_points_wh = np.where(section_time_key_total >= quote_key_times[qkt])[0]
                                    if quotes_critical_points_wh.size > 0:
                                        quotes_critical_points_wh = quotes_critical_points_wh[np.nanargmin(quotes_critical_points_wh)]
                                    quotes_critical_points = np.append(quotes_critical_points, quotes_critical_points_wh).astype(np.int64)
                                elif qkt == len(quote_key_times)-1:
                                    quotes_critical_points_wh = np.where(section_time_key_total <= quote_key_times[-1])[0]
                                    if quotes_critical_points_wh.size > 0:
                                        quotes_critical_points_wh = quotes_critical_points_wh[np.nanargmax(quotes_critical_points_wh)]
                                    quotes_critical_points = np.append(quotes_critical_points, quotes_critical_points_wh).astype(np.int64)
                                if quotes_critical_points.size > 11:
                                    quotes_critical_points = quotes_critical_points[:11]
                        return quotes_critical_points
                    quotes_critical_points = for_17(key_total,quote_key_times)



                    quotes_critical_points = quotes_critical_points.astype(np.int64)
                    section_askPrice_key_total = key_total.askPrice.values.astype(np.float64)
                    section_bidPrice_key_total = key_total.bidPrice.values.astype(np.float64)
                    section_ba_spread_total = np.divide(np.subtract(section_askPrice_key_total, section_bidPrice_key_total), section_askPrice_key_total)
                    section_askSize_key_total = key_total.askSize.values.astype(np.float64)
                    section_bidSize_key_total = key_total.bidSize.values.astype(np.float64)
                    section_basize_ratio_total = np.divide(section_bidSize_key_total, section_askSize_key_total)
                    section_mf_ask_total = np.multiply(section_askPrice_key_total, section_askSize_key_total)
                    section_mf_bid_total = np.multiply(section_bidPrice_key_total, section_bidSize_key_total)
                    section_mf_ba_ratio_total = np.divide(section_mf_bid_total, section_mf_ask_total)

                    avg_askPrice = np.nanmean(section_askPrice_key_total)
                    avg_bidPrice = np.nanmean(section_bidPrice_key_total)

                    current_askSize = section_askSize_key_total[-1]
                    current_bidSize = section_bidSize_key_total[-1]
                    avg_askSize = np.nanmean(section_askSize_key_total)
                    avg_bidSize = np.nanmean(section_bidSize_key_total)



                    key_neg = quote_key[(quote_key.index >= datetime.fromtimestamp(quote_key_times[0], pytz.utc)) & (quote_key.index <= datetime.fromtimestamp(quote_key_times[-3], pytz.utc))]
                    section_time_key_neg = key_neg.index.values.astype(int)
                    section_askPrice_key_neg = key_neg.askPrice.values.astype(np.float64)
                    section_bidPrice_key_neg = key_neg.bidPrice.values.astype(np.float64)
                    section_ba_spread_neg = np.divide(np.subtract(section_askPrice_key_neg, section_bidPrice_key_neg), section_askPrice_key_neg)
                    section_askSize_key_neg = key_neg.askSize.values.astype(np.float64)
                    section_bidSize_key_neg = key_neg.bidSize.values.astype(np.float64)
                    section_basize_ratio_neg = np.divide(section_bidSize_key_neg, section_askSize_key_neg)
                    section_mf_ask_neg = np.multiply(section_askPrice_key_neg, section_askSize_key_neg)
                    section_mf_bid_neg = np.multiply(section_bidPrice_key_neg, section_bidSize_key_neg)
                    section_mf_ba_ratio_neg = np.divide(section_mf_bid_neg, section_mf_ask_neg)

                    if section_mf_ask_neg.size > 0:
                        X = pd.DataFrame(section_time_key_neg)
                        y = pd.DataFrame(section_mf_ask_neg)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_mf_ask_neg_hat = np.array(lm.predict(X)).flatten()
                        mf_ask_total_pct_chg_neg = np.divide(np.subtract(section_mf_ask_neg_hat[-1], section_mf_ask_neg_hat[0]), section_mf_ask_neg_hat[0])
                        mf_ask_avg_roc_neg = np.nanmean((np.diff(section_mf_ask_neg_hat)/section_mf_ask_neg_hat[:-1])/np.diff(section_time_key_neg))
                        mf_ask_r2_neg = lm.score(X,y)
                    else:
                        mf_ask_total_pct_chg_neg = np.nan
                        mf_ask_avg_roc_neg = np.nan
                        mf_ask_r2_neg = np.nan

                    if section_mf_bid_neg.size > 0:
                        X = pd.DataFrame(section_time_key_neg)
                        y = pd.DataFrame(section_mf_bid_neg)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_mf_bid_neg_hat = np.array(lm.predict(X)).flatten()
                        mf_bid_total_pct_chg_neg = np.divide(np.subtract(section_mf_bid_neg_hat[-1], section_mf_bid_neg_hat[0]), section_mf_bid_neg_hat[0])
                        mf_bid_avg_roc_neg = np.nanmean((np.diff(section_mf_bid_neg_hat)/section_mf_bid_neg_hat[:-1])/np.diff(section_time_key_neg))
                        mf_bid_r2_neg = lm.score(X,y)
                    else:
                        mf_bid_total_pct_chg_neg = np.nan
                        mf_bid_avg_roc_neg = np.nan
                        mf_bid_r2_neg = np.nan

                    if section_ba_spread_neg.size > 0:
                        X = pd.DataFrame(section_time_key_neg)
                        y = pd.DataFrame(section_ba_spread_neg)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_ba_spread_neg_hat = np.array(lm.predict(X)).flatten()
                        ba_spread_total_pct_chg_neg = np.divide(np.subtract(section_ba_spread_neg_hat[-1], section_ba_spread_neg_hat[0]), section_ba_spread_neg_hat[0])
                        ba_spread_r2_neg = lm.score(X,y)
                    else:
                        ba_spread_total_pct_chg_neg = np.nan
                        ba_spread_r2_neg = np.nan

                    if section_askPrice_key_neg.size > 0:
                        X = pd.DataFrame(section_time_key_neg)
                        y = pd.DataFrame(section_askPrice_key_neg)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_askPrice_neg_hat = np.array(lm.predict(X)).flatten()
                        askPrice_total_pct_chg_neg = np.divide(np.subtract(section_askPrice_neg_hat[-1], section_askPrice_neg_hat[0]), section_askPrice_neg_hat[0])
                        askPrice_avg_roc_neg = np.nanmean((np.diff(section_askPrice_neg_hat)/section_askPrice_neg_hat[:-1])/np.diff(section_time_key_neg))
                        askPrice_r2_neg = lm.score(X,y)
                    else:
                        askPrice_total_pct_chg_neg = np.nan
                        askPrice_avg_roc_neg = np.nan
                        askPrice_r2_neg = np.nan

                    if section_bidPrice_key_neg.size > 0:
                        X = pd.DataFrame(section_time_key_neg)
                        y = pd.DataFrame(section_bidPrice_key_neg)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_bidPrice_neg_hat = np.array(lm.predict(X)).flatten()
                        bidPrice_total_pct_chg_neg = np.divide(np.subtract(section_bidPrice_neg_hat[-1], section_bidPrice_neg_hat[0]), section_bidPrice_neg_hat[0])
                        bidPrice_avg_roc_neg = np.nanmean((np.diff(section_bidPrice_neg_hat)/section_bidPrice_neg_hat[:-1])/np.diff(section_time_key_neg))
                        bidPrice_r2_neg = lm.score(X,y)
                    else:
                        bidPrice_total_pct_chg_neg = np.nan
                        bidPrice_avg_roc_neg = np.nan
                        bidPrice_r2_neg = np.nan
                    


                    key_pos = quote_key[(quote_key.index >= datetime.fromtimestamp(quote_key_times[-3], pytz.utc)) & (quote_key.index <= datetime.fromtimestamp(quote_key_times[-1], pytz.utc))]
                    section_time_key_pos = key_pos.index.values.astype(int)
                    section_askPrice_key_pos = key_pos.askPrice.values.astype(np.float64)
                    section_bidPrice_key_pos = key_pos.bidPrice.values.astype(np.float64)
                    section_ba_spread_pos = np.divide(np.subtract(section_askPrice_key_pos, section_bidPrice_key_pos), section_askPrice_key_pos)
                    section_askSize_key_pos = key_pos.askSize.values.astype(np.float64)
                    section_bidSize_key_pos = key_pos.bidSize.values.astype(np.float64)
                    section_basize_ratio_pos = np.divide(section_bidSize_key_pos, section_askSize_key_pos)
                    section_mf_ask_pos = np.multiply(section_askPrice_key_pos, section_askSize_key_pos)
                    section_mf_bid_pos = np.multiply(section_bidPrice_key_pos, section_bidSize_key_pos)
                    section_mf_ba_ratio_pos = np.divide(section_mf_bid_pos, section_mf_ask_pos)


                    if section_mf_ask_pos.size > 0:
                        X = pd.DataFrame(section_time_key_pos)
                        y = pd.DataFrame(section_mf_ask_pos)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_mf_ask_pos_hat = np.array(lm.predict(X)).flatten()
                        mf_ask_total_pct_chg_pos = np.divide(np.subtract(section_mf_ask_pos_hat[-1], section_mf_ask_pos_hat[0]), section_mf_ask_pos_hat[0])
                        mf_ask_avg_roc_pos = np.nanmean((np.diff(section_mf_ask_pos_hat)/section_mf_ask_pos_hat[:-1])/np.diff(section_time_key_pos))
                        mf_ask_r2_pos = lm.score(X,y)
                    else:
                        mf_ask_total_pct_chg_pos = np.nan
                        mf_ask_avg_roc_pos = np.nan
                        mf_ask_r2_pos = np.nan

                    if section_mf_bid_pos.size > 0:
                        X = pd.DataFrame(section_time_key_pos)
                        y = pd.DataFrame(section_mf_bid_pos)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_mf_bid_pos_hat = np.array(lm.predict(X)).flatten()
                        mf_bid_total_pct_chg_pos = np.divide(np.subtract(section_mf_bid_pos_hat[-1], section_mf_bid_pos_hat[0]), section_mf_bid_pos_hat[0])
                        mf_bid_avg_roc_pos = np.nanmean((np.diff(section_mf_bid_pos_hat)/section_mf_bid_pos_hat[:-1])/np.diff(section_time_key_pos))
                        mf_bid_r2_pos = lm.score(X,y)
                    else:
                        mf_bid_total_pct_chg_pos = np.nan
                        mf_bid_avg_roc_pos = np.nan
                        mf_bid_r2_pos = np.nan

                    if section_ba_spread_pos.size> 0:
                        X = pd.DataFrame(section_time_key_pos)
                        y = pd.DataFrame(section_ba_spread_pos)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_ba_spread_pos_hat = np.array(lm.predict(X)).flatten()
                        ba_spread_total_pct_chg_pos = np.divide(np.subtract(section_ba_spread_pos_hat[-1], section_ba_spread_pos_hat[0]), section_ba_spread_pos_hat[0])
                        ba_spread_r2_pos = lm.score(X,y)
                    else:
                        ba_spread_total_pct_chg_pos = np.nan
                        ba_spread_r2_pos = np.nan

                    if section_askPrice_key_pos.size > 0:
                        X = pd.DataFrame(section_time_key_pos)
                        y = pd.DataFrame(section_askPrice_key_pos)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_askPrice_pos_hat = np.array(lm.predict(X)).flatten()
                        askPrice_total_pct_chg_pos = np.divide(np.subtract(section_askPrice_pos_hat[-1], section_askPrice_pos_hat[0]), section_askPrice_pos_hat[0])
                        askPrice_avg_roc_pos = np.nanmean((np.diff(section_askPrice_pos_hat)/section_askPrice_pos_hat[:-1])/np.diff(section_time_key_pos))
                        askPrice_r2_pos = lm.score(X,y)
                    else:
                        askPrice_total_pct_chg_pos = np.nan
                        askPrice_avg_roc_pos = np.nan
                        askPrice_r2_pos = np.nan

                    if section_bidPrice_key_pos.size > 0:
                        X = pd.DataFrame(section_time_key_pos)
                        y = pd.DataFrame(section_bidPrice_key_pos)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_bidPrice_pos_hat = np.array(lm.predict(X)).flatten()
                        bidPrice_total_pct_chg_pos = np.divide(np.subtract(section_bidPrice_pos_hat[-1], section_bidPrice_pos_hat[0]), section_bidPrice_pos_hat[0])
                        bidPrice_avg_roc_pos = np.nanmean((np.diff(section_bidPrice_pos_hat)/section_bidPrice_pos_hat[:-1])/np.diff(section_time_key_pos))
                        bidPrice_r2_pos = lm.score(X,y)
                    else:
                        bidPrice_total_pct_chg_pos = np.nan
                        bidPrice_avg_roc_pos = np.nan
                        bidPrice_r2_pos = np.nan






                    final_output.update(dict(sec_1_avg_askPrice = avg_askPrice))                              
                    final_output.update(dict(sec_1_avg_bidPrice = avg_bidPrice)) 
                    final_output.update(dict(sec_1_current_askSize = current_askSize))                              
                    final_output.update(dict(sec_1_current_bidSize = current_bidSize))   
                    final_output.update(dict(sec_1_avg_askSize = avg_askSize))                              
                    final_output.update(dict(sec_1_avg_bidSize = avg_bidSize))                                    
                    final_output.update(dict(sec_1_mf_ask_total_pct_chg_neg = mf_ask_total_pct_chg_neg))                                                                         
                    final_output.update(dict(sec_1_mf_ask_avg_roc_neg = mf_ask_avg_roc_neg))                                                                         
                    final_output.update(dict(sec_1_mf_ask_r2_neg = mf_ask_r2_neg))                                                                         
                    final_output.update(dict(sec_1_mf_bid_total_pct_chg_neg = mf_bid_total_pct_chg_neg))                                                                         
                    final_output.update(dict(sec_1_mf_bid_avg_roc_neg = mf_bid_avg_roc_neg))                                                                         
                    final_output.update(dict(sec_1_mf_bid_r2_neg = mf_bid_r2_neg))                                                                         
                    final_output.update(dict(sec_1_ba_spread_total_pct_chg_neg = ba_spread_total_pct_chg_neg))                                                                         
                    final_output.update(dict(sec_1_ba_spread_r2_neg = ba_spread_r2_neg))                                                                     
                    final_output.update(dict(sec_1_askPrice_total_pct_chg_neg = askPrice_total_pct_chg_neg))                                                                         
                    final_output.update(dict(sec_1_askPrice_avg_roc_neg = askPrice_avg_roc_neg))                                                                        
                    final_output.update(dict(sec_1_askPrice_r2_neg = askPrice_r2_neg))                                                                         
                    final_output.update(dict(sec_1_bidPrice_total_pct_chg_neg = bidPrice_total_pct_chg_neg))                                                                         
                    final_output.update(dict(sec_1_bidPrice_avg_roc_neg = bidPrice_avg_roc_neg))                                                                         
                    final_output.update(dict(sec_1_bidPrice_r2_neg = bidPrice_r2_neg))                                                                        
                    final_output.update(dict(sec_1_mf_ask_total_pct_chg_pos = mf_ask_total_pct_chg_pos))                                     
                    final_output.update(dict(sec_1_mf_ask_avg_roc_pos = mf_ask_avg_roc_pos))                                     
                    final_output.update(dict(sec_1_mf_ask_r2_pos = mf_ask_r2_pos))                                     
                    final_output.update(dict(sec_1_mf_bid_total_pct_chg_pos = mf_bid_total_pct_chg_pos))                                     
                    final_output.update(dict(sec_1_mf_bid_avg_roc_pos = mf_bid_avg_roc_pos))                                                                         
                    final_output.update(dict(sec_1_mf_bid_r2_pos = mf_bid_r2_pos))                                                                         
                    final_output.update(dict(sec_1_ba_spread_total_pct_chg_pos = ba_spread_total_pct_chg_pos))                                                                         
                    final_output.update(dict(sec_1_ba_spread_r2_pos = ba_spread_r2_pos))                                                                 
                    final_output.update(dict(sec_1_askPrice_total_pct_chg_pos = askPrice_total_pct_chg_pos))                                                                     
                    final_output.update(dict(sec_1_askPrice_avg_roc_pos = askPrice_avg_roc_pos))                                                                      
                    final_output.update(dict(sec_1_askPrice_r2_pos = askPrice_r2_pos))                                  
                    final_output.update(dict(sec_1_bidPrice_total_pct_chg_pos = bidPrice_total_pct_chg_pos))                                                                       
                    final_output.update(dict(sec_1_bidPrice_avg_roc_pos = bidPrice_avg_roc_pos))                                                                       
                    final_output.update(dict(sec_1_bidPrice_r2_pos = bidPrice_r2_pos))
                    final_output.update(dict(ts = None))
                    final_output.update(dict(final_y = None))



        
        elif zero_crossings.size == 1:
            critical_points = np.array([])
            sec_pct_chg = np.array([])
            sec_curve_pct_chg = np.array([])
            der_1 = np.array([])
            der_1 = np.append(der_1, np.zeros(8) + np.nan)
            der_2 = np.array([])
            der_2 = np.append(der_2, np.zeros(8) + np.nan)
            der_3 = np.array([])
            der_3 = np.append(der_3, np.zeros(8) + np.nan)
            sec_pct_chg = np.append(sec_pct_chg, np.zeros(8) + np.nan)
            sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(8) + np.nan)
            critical_points = np.append(critical_points, np.zeros(8) + np.nan)
            
            curve_der = section_der[int(zero_crossings[-1]):]
            if curve_der.size == 0:
                der_1 = np.append(der_1, np.zeros(2) + np.nan)
                der_2 = np.append(der_2, np.zeros(2) + np.nan)
                der_3 = np.append(der_3, np.zeros(2) + np.nan)
                sec_pct_chg = np.append(sec_pct_chg, np.zeros(2) + np.nan)
                sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(2) + np.nan)
                critical_points = np.append(critical_points, np.zeros(3) + np.nan)
            elif curve_der.size > 0:
                curve_der_sec_1 = curve_der
                curve_der_entry = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, 1)))
                curve_der_415 = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, .415)))
                critical_point_10 = curve_der_415
                critical_point_11 = curve_der_entry
                critical_points = np.append(critical_points, 0)
                critical_points = np.append(critical_points, critical_point_10).astype(np.int64)
                critical_points = np.append(critical_points, critical_point_11).astype(np.int64)
                

                critical_points = critical_points.astype(np.int64)

                curve_price = section_price[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
                curve_time = section_time[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
                curve_price_hat = section_price_hat[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
                curve_price_hat_diff_div_diff = np.diff(np.divide(np.diff(curve_price_hat), curve_price_hat[:-1])) 
                curve_der = np.divide(curve_price_hat_diff_div_diff, np.diff(curve_time)[:-1])
                curve_time_r = section_time[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
                curve_price_r = section_price[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
                curve_price_hat_r = section_price_hat[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
                curve_vol_r = section_vol[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]

                
                curve_der_diff = np.diff(curve_der)
                section_der_2 = np.divide(curve_der_diff, np.diff(curve_time[:-2]))
                


                critical_points_pos = np.array([critical_points[-3], critical_points[-2], critical_points[-1]]).astype(np.int64)
                @jit(parallel=True)
                def for_18(critical_points_pos,curve_der, der_1):                
                    for pnn in range(0, len(critical_points_pos)-1):
                        if critical_points_pos[pnn+1]+1 < len(curve_der):
                            section_mean = np.nanmean(curve_der[critical_points_pos[pnn]:critical_points_pos[pnn+1]+1])
                            if section_mean.size == 0:
                                section_mean = np.nan
                            der_1 = np.append(der_1, section_mean)
                        else:
                            section_mean = np.nanmean(curve_der[critical_points_pos[pnn]:critical_points_pos[pnn+1]])
                            if section_mean.size == 0:
                                section_mean = np.nan
                            der_1 = np.append(der_1, section_mean)
                    return der_1
                der_1 = for_18(critical_points_pos,curve_der, der_1)

                @jit(parallel=True)
                def for_19(critical_points_pos,section_der_2, der_2):                        
                    for pnn2 in range(0, len(critical_points_pos)-1):
                        section_mean = np.nanmean(section_der_2[critical_points_pos[pnn2]:critical_points_pos[pnn2+1]])
                        if section_mean == 0:
                            section_mean = np.nan
                        der_2 = np.append(der_2, section_mean)
                    return der_2
                der_2 = for_19(critical_points_pos,section_der_2, der_2)
                
                section_der_2_diff = np.diff(section_der_2)
                section_der_3 = np.divide(section_der_2_diff, np.diff(curve_time[:-3]))			
                
                @jit(parallel=True)
                def for_20(critical_points_pos,section_der_3, der_3):                                        
                    for pnn3 in range(0, len(critical_points_pos)-1):
                        section_mean = np.nanmean(section_der_3[critical_points_pos[pnn3]:critical_points_pos[pnn3+1]-1])
                        if section_mean == 0:
                            section_mean = np.nan
                        der_3 = np.append(der_3, np.nan)
                    return der_3
                der_3 = for_20(critical_points_pos,section_der_3, der_3)

                @jit(parallel=True)
                def for_21(critical_points_pos, curve_price_hat, sec_pct_chg):                                                
                    for s_p_c_2nn in range(0, len(critical_points_pos)-1):
                        if critical_points_pos[s_p_c_2nn+1]+1 < len(curve_price_hat):
                            section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2nn+1]+1], curve_price_hat[critical_points_pos[s_p_c_2nn]]), curve_price_hat[critical_points_pos[s_p_c_2nn]])
                            if section_pct_chg.size == 0:
                                section_pct_chg = np.nan
                            sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
                        else:
                            section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2nn+1]], curve_price_hat[critical_points_pos[s_p_c_2nn]]), curve_price_hat[critical_points_pos[s_p_c_2nn]])
                            if section_pct_chg.size == 0:
                                section_pct_chg = np.nan
                            sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
                    return sec_pct_chg
                sec_pct_chg = for_21(critical_points_pos, curve_price_hat, sec_pct_chg)

                @jit(parallel=True)
                def for_22(critical_points_pos, curve_price_hat, sec_curve_pct_chg): 
                    for s_c_p_c_2nn in range(0, len(critical_points_pos)-1):
                        if critical_points_pos[s_c_p_c_2nn+1]+1 < len(curve_price_hat):
                            section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2nn+1]+1], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
                            if section_curve_pct_chg.size == 0:
                                section_curve_pct_chg = np.nan
                            sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
                        else:
                            section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2nn+1]], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
                            if section_curve_pct_chg.size == 0:
                                section_curve_pct_chg = np.nan
                            sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
                    return sec_curve_pct_chg
                sec_curve_pct_chg = for_22(critical_points_pos, curve_price_hat, sec_curve_pct_chg)


            r_sq = np.array([])
            std_per_section = np.array([])
            residual_max_per_section = np.array([])
            residual_mean_per_section = np.array([])
            if curve_price_hat_r.size == 0:
                r_sq = np.append(r_sq, np.zeros(10) + np.nan)
                std_per_section = np.append(std_per_section, np.zeros(10) + np.nan)
                residual_max_per_section = np.append(residual_max_per_section, np.zeros(10) + np.nan)
                residual_mean_per_section = np.append(residual_mean_per_section, np.zeros(10) + np.nan)
            elif curve_price_hat_r.size > 0:

                @jit(parallel=True)
                def for_23(critical_points, r_sq, std_per_section, residual_max_per_section, residual_mean_per_section, curve_time_r, curve_price_r):
                    for r in range(0, len(critical_points)-1):
                        if critical_points[r] <= -9223372036854775 or critical_points[r] == np.nan:
                            r_sq = np.append(r_sq, np.nan)
                            std_per_section = np.append(std_per_section, np.nan)
                            residual_max_per_section = np.append(residual_max_per_section, np.nan)
                            residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
                            continue
                        else:
                            if critical_points[r+1] <= -9223372036854775 or critical_points[r+1] == np.nan:
                                r_sq = np.append(r_sq, np.nan)
                                std_per_section = np.append(std_per_section, np.nan)
                                residual_max_per_section = np.append(residual_max_per_section, np.nan)
                                residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
                                continue
                            else:
                                
                                if critical_points[r+1] == critical_points[r]:
                                    if r_sq.size == 0 or std_per_section.size == 0 or residual_max_per_section.size == 0 or residual_mean_per_section.size == 0 :
                                        r_sq = np.append(r_sq, np.nan)
                                        std_per_section = np.append(std_per_section, np.nan)
                                        residual_max_per_section = np.append(residual_max_per_section, np.nan)
                                        residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
                                    else:	
                                        r += 1
                                        r_sq = np.append(r_sq, r_sq[-1])
                                        std_per_section = np.append(std_per_section, std_per_section[-1])
                                        residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section[-1])
                                        residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section[-1])
                                        continue
                                else:
                                    r_sq_2 = model_pyearth.score(curve_time_r[critical_points[r]:critical_points[r+1]+2], curve_price_r[critical_points[r]:critical_points[r+1]+2])
                                    r_sq = np.append(r_sq, r_sq_2)
                                    avg_mean = np.nanmean(curve_price_r[critical_points[r]:critical_points[r+1]+2])
                                    std_sec = np.nanstd(curve_price_r[critical_points[r]:critical_points[r+1]+2])
                                    std_per_section = np.append(std_per_section, std_sec)
                                    curvy = curve_price_hat_r[critical_points[r]:critical_points[r+1]+2]
                                    residual_max_per_section_0_0_0 = np.abs(np.subtract(curve_price_r[critical_points[r]:critical_points[r+1]+2], curve_price_hat_r[critical_points[r]:critical_points[r+1]+2]))
                                    residual_max_per_section_0_0 = np.nanargmax(residual_max_per_section_0_0_0)
                                    residual_max_per_section_0 = np.take(residual_max_per_section_0_0_0, residual_max_per_section_0_0)
                                    residual_mean_per_section_0 = np.nanmean(np.abs(np.subtract(curve_price_r[critical_points[r]:critical_points[r+1]+2], curve_price_hat_r[critical_points[r]:critical_points[r+1]+2])))
                                    residual_max_per_section_0_ph = np.divide(residual_max_per_section_0, np.take(curvy, residual_max_per_section_0_0))
                                    residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section_0_ph)
                                    residual_mean_per_section_0_ph = np.nanmean(curvy)
                                    residual_mean_per_section_div = np.divide(residual_mean_per_section_0, residual_mean_per_section_0_ph)
                                    residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section_div)

                    return np.array([r_sq, std_per_section, residual_max_per_section, residual_mean_per_section])

                for_23_variables = for_23(critical_points, r_sq, std_per_section, residual_max_per_section, residual_mean_per_section, curve_time_r, curve_price_r)
                r_sq = for_23_variables[0]
                std_per_section = for_23_variables[1]
                residual_max_per_section = for_23_variables[2]
                residual_mean_per_section = for_23_variables[3]

            r_sq = r_sq.astype(np.float64)

            time_since_maxima = np.array([])
            time_since_last_sect = np.array([])
            if curve_price_hat_r.size == 0:
                time_since_maxima = np.append(time_since_maxima, np.zeros(10) + np.nan)
                time_since_last_sect = np.append(time_since_last_sect, np.zeros(10) + np.nan)
            elif curve_price_hat_r.size > 0:
                @jit(parallel=True)
                def for_24(critical_points, curve_time_r, time_since_maxima, time_since_last_sect):
                    for cnn in range(0, len(critical_points)-1):
                        if critical_points[cnn] <= -9223372036854775 or critical_points[cnn] == np.nan:
                            time_since_maxima = np.append(time_since_maxima, np.nan)
                            time_since_last_sect = np.append(time_since_last_sect, np.nan)
                            continue
                        else:
                            if critical_points[cnn+1] <= -9223372036854775 or critical_points[cnn+1] == np.nan:
                                time_since_maxima = np.append(time_since_maxima, np.nan)
                                time_since_last_sect = np.append(time_since_last_sect, np.nan)
                                continue
                            else:
                                if critical_points[cnn+1] == critical_points[cnn]:
                                    cnn += 1
                                    time_since_maxima = np.append(time_since_maxima, np.zeros(1))
                                    time_since_last_sect = np.append(time_since_last_sect, np.zeros(1))
                                    continue
                                else:
                                    if critical_points[0] <= -9223372036854775 or critical_points[0] == np.nan:
                                        time_since_maxima_append = np.nan
                                    else:
                                        if critical_points[cnn+1]+1 < len(curve_time_r)-1:
                                            time_since_maxima_append = curve_time_r[critical_points[cnn+1]+1] - curve_time_r[critical_points[0]]
                                            if time_since_maxima_append.size == 0:
                                                time_since_maxima_append = np.nan
                                            time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
                                            time_since_last_sect_append = curve_time_r[critical_points[cnn+1]+1] - curve_time_r[critical_points[cnn]]
                                            time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)
                                        else:
                                            time_since_maxima_append = curve_time_r[critical_points[cnn+1]] - curve_time_r[critical_points[0]]
                                            if time_since_maxima_append.size == 0:
                                                time_since_maxima_append = np.nan
                                            time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
                                            time_since_last_sect_append = curve_time_r[critical_points[cnn+1]] - curve_time_r[critical_points[cnn]]
                                            time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)
                    return np.array([time_since_maxima, time_since_last_sect])
            
                for_24_variables = for_24(critical_points, curve_time_r, time_since_maxima, time_since_last_sect)
                time_since_maxima = for_24_variables[0]
                time_since_last_sect = for_24_variables[1]


            residual_max_2 = np.abs(np.subtract(curve_price_r, curve_price_hat_r))
            residual_max_1 = np.nanargmax(residual_max_2)
            residual_max_0 = np.take(residual_max_2, residual_max_1)
            residual_max_0_ph = np.take(curve_price_hat_r, residual_max_1)
            residual_max = np.divide(residual_max_0, residual_max_0_ph)
            residual_mean_0 = np.nanmean(np.abs(np.subtract(curve_price_r, curve_price_hat_r)))
            residual_mean = np.divide(residual_mean_0, np.nanmean(curve_price_hat_r))



            if critical_points.size > 0 and curve_price_r.size > 0:
                current_price = curve_price_r[-1]
            else:
                current_price = np.nan
            
            if critical_points.size > 0 and curve_price_hat_r.size > 0:
                current_price_hat_r = curve_price_hat_r[-1]
            else:
                current_price_hat_r = np.nan

            if curve_price_r.size > 0:
                avg_Price = np.nanmean(curve_price_r)
            else:
                avg_Price = np.nan

            current_datetime = np.multiply(curve_time[-1],1000000000).astype(np.int64).item()
            current_date = datetime(1970, 1, 1) + timedelta(seconds=current_datetime)
            current_date = current_date.strftime('%Y-%m-%d')
            open_ts = datetime.strptime(current_date + ' 13:30:00', '%Y-%m-%d %H:%M:%S')
            dt = pytz.timezone('America/Chicago').normalize(open_ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Chicago')))
            is_dst = bool(dt.dst())
            if is_dst is True:
                offset_open = int(time.mktime(time.strptime(current_date + ' 13:30:00', '%Y-%m-%d %H:%M:%S')))
            elif is_dst is False:
                offset_open = int(time.mktime(time.strptime(current_date + ' 14:30:00', '%Y-%m-%d %H:%M:%S')))
            if critical_points.size > 0 and curve_time_r.size > 0:
                current_unix_time = np.multiply(curve_time_r[-1],1000000000)
                ms_since_open = np.multiply(curve_time_r[-1],1000000000) - offset_open
            else:
                current_unix_time = np.nan
                ms_since_open = np.nan

            current_date = datetime.strptime(current_date, '%Y-%m-%d')
            

            first_day = current_date.replace(day=1)
            dom = current_date.day
            adjusted_dom = dom + first_day.weekday()


            

            total_days = int((current_date - datetime(1970,1,1)).days)



            
            price_pcavg_per_section = np.array([])
            if curve_price_hat_r.size == 0:
                price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(10) + np.nan)
            elif curve_price_hat_r.size > 0:
                @jit(parallel=True)
                def for_25(critical_points, curve_price_hat_r, price_pcavg_per_section):
                    for avg_p_n_2 in range(0, len(critical_points)-1):
                        if critical_points[avg_p_n_2] <= -9223372036854775 or critical_points[avg_p_n_2] == np.nan:
                            price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
                            continue
                        else:
                            if critical_points[avg_p_n_2+1] <= -9223372036854775 or critical_points[avg_p_n_2+1] == np.nan:
                                price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
                                continue
                            else:
                                if critical_points[avg_p_n_2+1] == critical_points[avg_p_n_2]:
                                    avg_p_n_2 += 1
                                    price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(1))
                                    continue
                                else:
                                    price_pcavg_per_section = np.append(price_pcavg_per_section, np.nanmean(np.diff(curve_price_hat_r[critical_points[avg_p_n_2]:critical_points[avg_p_n_2+1]+2])/curve_price_hat_r[critical_points[avg_p_n_2]:critical_points[avg_p_n_2+1]+2][:-1]))
                    return price_pcavg_per_section


                    price_pcavg_per_section = for_25(critical_points, curve_price_hat_r, price_pcavg_per_section)


            price_pct_chg_neg = np.nan
            price_pct_chg_pos = np.divide(np.subtract(curve_price[-1], curve_price[0]), curve_price[0])


            lm = linear_model.LinearRegression()

            lr_price_total_pct_chg_neg = np.nan
            lr_price_avg_roc_neg = np.nan
            lr_price_r2_neg = np.nan




            X = pd.DataFrame(curve_time)
            y = pd.DataFrame(curve_price)
            y =  Imputer().fit_transform(y)
            model_lm = lm.fit(X,y)
            lr_curve_pos_hat = np.array(lm.predict(X)).flatten()
            lr_price_total_pct_chg_pos = np.divide(np.subtract(lr_curve_pos_hat[-1], lr_curve_pos_hat[0]), lr_curve_pos_hat[0])
            lr_price_avg_roc_pos = np.nanmean((np.diff(lr_curve_pos_hat)/lr_curve_pos_hat[:-1])/np.diff(curve_time))
            lr_price_r2_pos = lm.score(X,y)



            lr_price_pct_chg_per_section = np.array([])
            lr_price_roc_per_section = np.array([])
            lr_price_r2_per_section = np.array([])
            if curve_price_r.size == 0:
                lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(10) + np.nan)
                lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(10) + np.nan)
                lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(10) + np.nan)
            elif curve_price_r.size > 0:
                @jit(parallel=True)
                def for_26(critical_points, lr_price_pct_chg_per_section, lr_price_roc_per_section, lr_price_r2_per_section, curve_time_r, curve_price_r):
                    for avg_p_2 in range(0, len(critical_points)-1):
                        if critical_points[avg_p_2] <= -9223372036854775 or critical_points[avg_p_2] == np.nan:
                            lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
                            lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
                            lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
                            continue
                        else:
                            if critical_points[avg_p_2+1] <= -9223372036854775 or critical_points[avg_p_2+1] == np.nan:
                                lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
                                lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
                                lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
                                continue
                            else:
                                if critical_points[avg_p_2+1] == critical_points[avg_p_2]:
                                    avg_p_2 += 1
                                    lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(1))
                                    lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(1))
                                    lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(1))
                                    continue
                                else:
                                    X = pd.DataFrame(curve_time_r[critical_points[avg_p_2]:critical_points[avg_p_2+1]+2])
                                    y = pd.DataFrame(curve_price_r[critical_points[avg_p_2]:critical_points[avg_p_2+1]+2])
                                    y =  Imputer().fit_transform(y)
                                    model_lm = lm.fit(X,y)
                                    lr_price_per_section_hat = np.array(lm.predict(X)).flatten()
                                    lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.divide(np.subtract(lr_price_per_section_hat[-1], lr_price_per_section_hat[0]), lr_price_per_section_hat[0]))
                                    lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nanmean((np.diff(lr_price_per_section_hat)/lr_price_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[avg_p_2]:critical_points[avg_p_2+1]+2])))
                                    lr_price_r2_per_section_2 = lm.score(X,y)
                                    lr_price_r2_per_section = np.append(lr_price_r2_per_section, lr_price_r2_per_section_2)
                    return np.array([lr_price_pct_chg_per_section, lr_price_roc_per_section, lr_price_r2_per_section])
                for_26_variables = for_26(critical_points, lr_price_pct_chg_per_section, lr_price_roc_per_section, lr_price_r2_per_section, curve_time_r, curve_price_r)
                lr_price_pct_chg_per_section = for_26_variables[0]
                lr_price_roc_per_section = for_26_variables[1]
                lr_price_r2_per_section = for_26_variables[2]



            vol_total_pct_chg_neg = np.nan
            vol_r2_neg = np.nan



            section_time_key_pos = curve_time_r[int(critical_points[-3]):int(critical_points[-1]+2)]
            section_vol_key_pos = curve_vol_r[int(critical_points[-3]):int(critical_points[-1]+2)]

            X = pd.DataFrame(section_time_key_pos)
            y = pd.DataFrame(section_vol_key_pos)
            y =  Imputer().fit_transform(y)
            model_lm = lm.fit(X,y)
            section_vol_pos_hat = np.array(lm.predict(X)).flatten()
            vol_total_pct_chg_pos = np.divide(np.subtract(section_vol_pos_hat[-1], section_vol_pos_hat[0]), section_vol_pos_hat[0])
            vol_r2_pos = lm.score(X,y)




            section_mf_key_total = np.multiply(curve_price_r, curve_vol_r)


            mf_price_r2_neg = np.nan




            section_time_key_pos = curve_time
            section_mf_price_key_pos = np.multiply(curve_price, section_vol_key_pos)

            X = pd.DataFrame(section_time_key_pos)
            y = pd.DataFrame(section_mf_price_key_pos)
            y =  Imputer().fit_transform(y)
            model_lm = lm.fit(X,y)
            section_mf_price_pos_hat = np.array(lm.predict(X)).flatten()
            mf_price_r2_pos = lm.score(X,y)



            avg_vol = np.nanmean(curve_vol_r)

            
            quote_key_times_append = np.array([])
            quote_key_times = np.array([])
            @jit(parallel=True)
            def for_27(critical_points, quote_key_times_append, quote_key_times, curve_time_r):
                for cpnn in range(0, len(critical_points)):
                    if critical_points[cpnn] <= -9223372036854775 or critical_points[cpnn] == np.nan:
                        quote_key_times = np.append(quote_key_times, np.nan)
                        continue
                    else:
                        if critical_points[cpnn]+1 < len(curve_time_r) and cpnn == 0:
                            quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cpnn])], 1000000000)
                            if quote_key_times_append.size == 0:
                                quote_key_times_append = np.nan
                            quote_key_times = np.append(quote_key_times, quote_key_times_append)
                        elif critical_points[cpnn]+1 < len(curve_time_r) and cpnn > 0:
                            quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cpnn]+1)], 1000000000)
                            if quote_key_times_append.size == 0:
                                quote_key_times_append = np.nan
                            quote_key_times = np.append(quote_key_times, quote_key_times_append)
                        elif critical_points[cpnn]+1 >= len(curve_time_r):
                            quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cpnn])], 1000000000)
                            if quote_key_times_append.size == 0:
                                quote_key_times_append = np.nan
                            quote_key_times = np.append(quote_key_times, quote_key_times_append)
                return quote_key_times
            quote_key_times = for_27(critical_points, quote_key_times_append, quote_key_times, curve_time_r)











            final_output.update(dict(symbol = symbol))
            final_output.update(dict(avg_return_40_mins = avg_return_40_mins)) 
            final_output.update(dict(avg_chg_in_avg_return = avg_chg_in_avg_return)) 
            final_output.update(dict(return_minus_5 = return_minus_5)) 
            final_output.update(dict(return_minus_4 = return_minus_4)) 
            final_output.update(dict(return_minus_3 = return_minus_3)) 
            final_output.update(dict(return_minus_2 = return_minus_2)) 
            final_output.update(dict(lr_all_day_time_passed = lr_all_day_time_passed)) 
            final_output.update(dict(lr_all_day_pct_chg = lr_all_day_pct_chg))
            final_output.update(dict(lr_all_day_r2 = lr_all_day_r2)) 
            final_output.update(dict(zip(['sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1'], der_1.T)))
            final_output.update(dict(zip(['sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2'], der_2.T)))
            final_output.update(dict(zip(['sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3'], der_3.T)))
            final_output.update(dict(zip(['sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq'], r_sq.T)))   
            final_output.update(dict(zip(['sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section'], std_per_section.T)))   
            final_output.update(dict(zip(['sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section'], residual_max_per_section.T)))   
            final_output.update(dict(zip(['sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section'], residual_mean_per_section.T)))   
            final_output.update(dict(zip(['sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg'], sec_pct_chg.T)))
            final_output.update(dict(zip(['sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg'], sec_curve_pct_chg.T)))
            final_output.update(dict(zip(['sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima'], time_since_maxima.T)))
            final_output.update(dict(zip(['sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect'], time_since_last_sect.T))) 
            final_output.update(dict(sec_1_residual_max = residual_max))
            final_output.update(dict(sec_1_residual_mean = residual_mean))				
            final_output.update(dict(sec_1_current_unix_time = current_unix_time)) 
            final_output.update(dict(sec_1_ms_since_open = ms_since_open)) 
            final_output.update(dict(total_days = total_days)) 
            final_output.update(dict(sec_1_current_price = current_price))
            final_output.update(dict(sec_1_current_price_hat_r = current_price_hat_r)) 					 
            final_output.update(dict(sec_1_avg_price = avg_Price)) 
            final_output.update(dict(zip(['sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section'], price_pcavg_per_section.T))) 
            final_output.update(dict(zip(['sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section'], lr_price_pct_chg_per_section.T))) 
            final_output.update(dict(zip(['sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section'], lr_price_roc_per_section.T))) 
            final_output.update(dict(zip(['sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section'], lr_price_r2_per_section.T))) 
            final_output.update(dict(sec_1_price_pct_chg_neg = price_pct_chg_neg)) 
            final_output.update(dict(sec_1_price_pct_chg_pos = price_pct_chg_pos)) 
            final_output.update(dict(sec_1_lr_price_total_pct_chg_neg = lr_price_total_pct_chg_neg)) 
            final_output.update(dict(sec_1_lr_price_avg_roc_neg = lr_price_avg_roc_neg)) 
            final_output.update(dict(sec_1_lr_price_r2_neg = lr_price_r2_neg)) 
            final_output.update(dict(sec_1_lr_price_total_pct_chg_pos = lr_price_total_pct_chg_pos)) 
            final_output.update(dict(sec_1_lr_price_avg_roc_pos = lr_price_avg_roc_pos)) 
            final_output.update(dict(sec_1_lr_price_r2_pos = lr_price_r2_pos)) 
            final_output.update(dict(sec_1_vol_total_pct_chg_neg = vol_total_pct_chg_neg)) 
            final_output.update(dict(sec_1_vol_total_pct_chg_pos = vol_total_pct_chg_pos)) 
            final_output.update(dict(sec_1_vol_r2_neg = vol_r2_neg)) 
            final_output.update(dict(sec_1_vol_r2_pos = vol_r2_pos)) 
            final_output.update(dict(sec_1_mf_price_r2_neg = mf_price_r2_neg)) 
            final_output.update(dict(sec_1_mf_price_r2_pos = mf_price_r2_pos)) 
            final_output.update(dict(sec_1_avg_vol = avg_vol)) 








            start_idx_price = price_pd.index[0]
            end_idx_price = price_pd.index[-1]
            quote_key = pd.DataFrame({'A' : []})
            #for quotes_side in quotes_side:
            if quotes_side.empty is False:
                #if quotes_side['Symbol'][0] == price_pd['Symbol'][0] and quotes_side.index[0].date() == price_pd.index[0].date():
                start_idx_quote = quotes_side.index.get_loc(start_idx_price, method='bfill')
                #start_idx_quote = (quotes_side.index-start_idx_price).abs().idxmin()
                end_idx_quote = quotes_side.index.get_loc(end_idx_price, method='ffill')
                #end_idx_quote = (quotes_side.index-end_idx_price).abs().idxmin()
                quote_key = quotes_side.iloc[start_idx_quote:end_idx_quote+1,:]


            if quote_key.empty is True:
                avg_askPrice = np.nan                          
                avg_bidPrice = np.nan
                current_askSize = np.nan
                current_bidSize = np.nan  			                               
                avg_askSize = np.nan                          
                avg_bidSize = np.nan                   
                mf_ask_total_pct_chg_neg = np.nan                                                                         
                mf_ask_avg_roc_neg = np.nan                                                                        
                mf_ask_r2_neg = np.nan                                                                         
                mf_bid_total_pct_chg_neg = np.nan                                                                         
                mf_bid_avg_roc_neg = np.nan                                                                         
                mf_bid_r2_neg = np.nan                                                                         
                ba_spread_total_pct_chg_neg = np.nan                                                                         
                ba_spread_r2_neg = np.nan                                                                       
                askPrice_total_pct_chg_neg = np.nan                                                                         
                askPrice_avg_roc_neg = np.nan                                                                        
                askPrice_r2_neg = np.nan                                                                         
                bidPrice_total_pct_chg_neg = np.nan                                                                         
                bidPrice_avg_roc_neg = np.nan                                                                         
                bidPrice_r2_neg = np.nan                                                              
                mf_ask_total_pct_chg_pos = np.nan                                     
                mf_ask_avg_roc_pos = np.nan                                     
                mf_ask_r2_pos = np.nan                                     
                mf_bid_total_pct_chg_pos = np.nan                                     
                mf_bid_avg_roc_pos = np.nan                                                                         
                mf_bid_r2_pos = np.nan                                                                         
                ba_spread_total_pct_chg_pos = np.nan                                                                         
                ba_spread_r2_pos = np.nan                                                                   
                askPrice_total_pct_chg_pos = np.nan                                                                      
                askPrice_avg_roc_pos = np.nan                                                                       
                askPrice_r2_pos = np.nan                                   
                bidPrice_total_pct_chg_pos = np.nan                                                                        
                bidPrice_avg_roc_pos = np.nan                                                                        
                bidPrice_r2_pos = np.nan


                final_output.update(dict(sec_1_avg_askPrice = avg_askPrice))                              
                final_output.update(dict(sec_1_avg_bidPrice = avg_bidPrice)) 
                final_output.update(dict(sec_1_current_askSize = current_askSize))                              
                final_output.update(dict(sec_1_current_bidSize = current_bidSize))   
                final_output.update(dict(sec_1_avg_askSize = avg_askSize))                              
                final_output.update(dict(sec_1_avg_bidSize = avg_bidSize))                                     
                final_output.update(dict(sec_1_mf_ask_total_pct_chg_neg = mf_ask_total_pct_chg_neg))                                                                         
                final_output.update(dict(sec_1_mf_ask_avg_roc_neg = mf_ask_avg_roc_neg))                                                                         
                final_output.update(dict(sec_1_mf_ask_r2_neg = mf_ask_r2_neg))                                                                         
                final_output.update(dict(sec_1_mf_bid_total_pct_chg_neg = mf_bid_total_pct_chg_neg))                                                                         
                final_output.update(dict(sec_1_mf_bid_avg_roc_neg = mf_bid_avg_roc_neg))                                                                         
                final_output.update(dict(sec_1_mf_bid_r2_neg = mf_bid_r2_neg))                                                                         
                final_output.update(dict(sec_1_ba_spread_total_pct_chg_neg = ba_spread_total_pct_chg_neg))                                                                         
                final_output.update(dict(sec_1_ba_spread_r2_neg = ba_spread_r2_neg))                                                                         
                final_output.update(dict(sec_1_askPrice_total_pct_chg_neg = askPrice_total_pct_chg_neg))                                                                         
                final_output.update(dict(sec_1_askPrice_avg_roc_neg = askPrice_avg_roc_neg))                                                                        
                final_output.update(dict(sec_1_askPrice_r2_neg = askPrice_r2_neg))                                                                         
                final_output.update(dict(sec_1_bidPrice_total_pct_chg_neg = bidPrice_total_pct_chg_neg))                                                                         
                final_output.update(dict(sec_1_bidPrice_avg_roc_neg = bidPrice_avg_roc_neg))                                                                         
                final_output.update(dict(sec_1_bidPrice_r2_neg = bidPrice_r2_neg))                                                                       
                final_output.update(dict(sec_1_mf_ask_total_pct_chg_pos = mf_ask_total_pct_chg_pos))                                     
                final_output.update(dict(sec_1_mf_ask_avg_roc_pos = mf_ask_avg_roc_pos))                                     
                final_output.update(dict(sec_1_mf_ask_r2_pos = mf_ask_r2_pos))                                     
                final_output.update(dict(sec_1_mf_bid_total_pct_chg_pos = mf_bid_total_pct_chg_pos))                                     
                final_output.update(dict(sec_1_mf_bid_avg_roc_pos = mf_bid_avg_roc_pos))                                                                         
                final_output.update(dict(sec_1_mf_bid_r2_pos = mf_bid_r2_pos))                                                                         
                final_output.update(dict(sec_1_ba_spread_total_pct_chg_pos = ba_spread_total_pct_chg_pos))                                                                         
                final_output.update(dict(sec_1_ba_spread_r2_pos = ba_spread_r2_pos))                                                                
                final_output.update(dict(sec_1_askPrice_total_pct_chg_pos = askPrice_total_pct_chg_pos))                                                                     
                final_output.update(dict(sec_1_askPrice_avg_roc_pos = askPrice_avg_roc_pos))                                                                      
                final_output.update(dict(sec_1_askPrice_r2_pos = askPrice_r2_pos))                                  
                final_output.update(dict(sec_1_bidPrice_total_pct_chg_pos = bidPrice_total_pct_chg_pos))                                                                       
                final_output.update(dict(sec_1_bidPrice_avg_roc_pos = bidPrice_avg_roc_pos))                                                                       
                final_output.update(dict(sec_1_bidPrice_r2_pos = bidPrice_r2_pos))
                final_output.update(dict(ts = None))
                final_output.update(dict(final_y = None))


            elif quote_key.empty is False:
                key_total = quote_key[(quote_key.index >= datetime.fromtimestamp(quote_key_times[-3], pytz.utc)) & (quote_key.index <= datetime.fromtimestamp(quote_key_times[-1], pytz.utc))]						
                if key_total.empty is False:
                    section_time_key_total = key_total.index.values.astype(int)
                    section_time_key_total = np.divide(section_time_key_total, 100000000)
                    quotes_critical_points = np.array([])
                    @jit(parallel=True)
                    def for_28(quotes_critical_points, quote_key_times, section_time_key_total):
                        for qktnn in range(0, len(quote_key_times)):
                            if quote_key_times[qktnn] <= -9223372036854775 or quote_key_times[qktnn] == np.nan:
                                quotes_critical_points = np.append(quotes_critical_points, np.nan)
                                continue
                            
                            else:
                                if qktnn < len(quote_key_times)-1:
                                    quotes_critical_points_wh = np.where(section_time_key_total >= quote_key_times[qktnn])[0]
                                    if quotes_critical_points_wh.size > 0:
                                        quotes_critical_points_wh = quotes_critical_points_wh[np.nanargmin(quotes_critical_points_wh)]
                                    quotes_critical_points = np.append(quotes_critical_points, quotes_critical_points_wh).astype(np.int64)
                                elif qktnn == len(quote_key_times)-1:
                                    quotes_critical_points_wh = np.where(section_time_key_total <= quote_key_times[-1])[0]
                                    if quotes_critical_points_wh.size > 0:
                                        quotes_critical_points_wh = quotes_critical_points_wh[np.nanargmax(quotes_critical_points_wh)]
                                    quotes_critical_points = np.append(quotes_critical_points, quotes_critical_points_wh).astype(np.int64)
                                if quotes_critical_points.size > 11:
                                    quotes_critical_points = quotes_critical_points[:11]
                        return quotes_critical_points
                    quotes_critical_points = for_28(quotes_critical_points, quote_key_times, section_time_key_total)
                    quotes_critical_points = quotes_critical_points.astype(np.int64)
                    section_askPrice_key_total = key_total.askPrice.values.astype(np.float64)
                    section_bidPrice_key_total = key_total.bidPrice.values.astype(np.float64)
                    section_ba_spread_total = np.divide(np.subtract(section_askPrice_key_total, section_bidPrice_key_total), section_askPrice_key_total)
                    section_askSize_key_total = key_total.askSize.values.astype(np.float64)
                    section_bidSize_key_total = key_total.bidSize.values.astype(np.float64)
                    section_basize_ratio_total = np.divide(section_bidSize_key_total, section_askSize_key_total)
                    section_mf_ask_total = np.multiply(section_askPrice_key_total, section_askSize_key_total)
                    section_mf_bid_total = np.multiply(section_bidPrice_key_total, section_bidSize_key_total)
                    section_mf_ba_ratio_total = np.divide(section_mf_bid_total, section_mf_ask_total)

                    avg_askPrice = np.nanmean(section_askPrice_key_total)
                    avg_bidPrice = np.nanmean(section_bidPrice_key_total)

                    current_askSize = section_askSize_key_total[-1]
                    current_bidSize = section_bidSize_key_total[-1]
                    avg_askSize = np.nanmean(section_askSize_key_total)
                    avg_bidSize = np.nanmean(section_bidSize_key_total)


                    mf_ask_total_pct_chg_neg = np.nan
                    mf_ask_avg_roc_neg = np.nan
                    mf_ask_r2_neg = np.nan

                    mf_bid_total_pct_chg_neg = np.nan
                    mf_bid_avg_roc_neg = np.nan
                    mf_bid_r2_neg = np.nan

                    
                    ba_spread_total_pct_chg_neg = np.nan
                    ba_spread_r2_neg = np.nan

                    
                    askPrice_total_pct_chg_neg = np.nan
                    askPrice_avg_roc_neg = np.nan
                    askPrice_r2_neg = np.nan

                    
                    bidPrice_total_pct_chg_neg = np.nan
                    bidPrice_avg_roc_neg = np.nan
                    bidPrice_r2_neg = np.nan
                    

                    key_pos = quote_key[(quote_key.index >= datetime.fromtimestamp(quote_key_times[-3], pytz.utc)) & (quote_key.index <= datetime.fromtimestamp(quote_key_times[-1], pytz.utc))]
                    section_time_key_pos = key_pos.index.values.astype(int)
                    section_askPrice_key_pos = key_pos.askPrice.values.astype(np.float64)
                    section_bidPrice_key_pos = key_pos.bidPrice.values.astype(np.float64)
                    section_ba_spread_pos = np.divide(np.subtract(section_askPrice_key_pos, section_bidPrice_key_pos), section_askPrice_key_pos)
                    section_askSize_key_pos = key_pos.askSize.values.astype(np.float64)
                    section_bidSize_key_pos = key_pos.bidSize.values.astype(np.float64)
                    section_basize_ratio_pos = np.divide(section_bidSize_key_pos, section_askSize_key_pos)
                    section_mf_ask_pos = np.multiply(section_askPrice_key_pos, section_askSize_key_pos)
                    section_mf_bid_pos = np.multiply(section_bidPrice_key_pos, section_bidSize_key_pos)
                    section_mf_ba_ratio_pos = np.divide(section_mf_bid_pos, section_mf_ask_pos)


                    if section_mf_ask_pos.size > 0:
                        X = pd.DataFrame(section_time_key_pos)
                        y = pd.DataFrame(section_mf_ask_pos)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_mf_ask_pos_hat = np.array(lm.predict(X)).flatten()
                        mf_ask_total_pct_chg_pos = np.divide(np.subtract(section_mf_ask_pos_hat[-1], section_mf_ask_pos_hat[0]), section_mf_ask_pos_hat[0])
                        mf_ask_avg_roc_pos = np.nanmean((np.diff(section_mf_ask_pos_hat)/section_mf_ask_pos_hat[:-1])/np.diff(section_time_key_pos))
                        mf_ask_r2_pos = lm.score(X,y)
                    else:
                        mf_ask_total_pct_chg_pos = np.nan
                        mf_ask_avg_roc_pos = np.nan
                        mf_ask_r2_pos = np.nan

                    if section_mf_bid_pos.size > 0:
                        X = pd.DataFrame(section_time_key_pos)
                        y = pd.DataFrame(section_mf_bid_pos)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_mf_bid_pos_hat = np.array(lm.predict(X)).flatten()
                        mf_bid_total_pct_chg_pos = np.divide(np.subtract(section_mf_bid_pos_hat[-1], section_mf_bid_pos_hat[0]), section_mf_bid_pos_hat[0])
                        mf_bid_avg_roc_pos = np.nanmean((np.diff(section_mf_bid_pos_hat)/section_mf_bid_pos_hat[:-1])/np.diff(section_time_key_pos))
                        mf_bid_r2_pos = lm.score(X,y)
                    else:
                        mf_bid_total_pct_chg_pos = np.nan
                        mf_bid_avg_roc_pos = np.nan
                        mf_bid_r2_pos = np.nan

                    if section_ba_spread_pos.size> 0:
                        X = pd.DataFrame(section_time_key_pos)
                        y = pd.DataFrame(section_ba_spread_pos)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_ba_spread_pos_hat = np.array(lm.predict(X)).flatten()
                        ba_spread_total_pct_chg_pos = np.divide(np.subtract(section_ba_spread_pos_hat[-1], section_ba_spread_pos_hat[0]), section_ba_spread_pos_hat[0])
                        ba_spread_r2_pos = lm.score(X,y)
                    else:
                        ba_spread_total_pct_chg_pos = np.nan
                        ba_spread_r2_pos = np.nan

                    if section_askPrice_key_pos.size > 0:
                        X = pd.DataFrame(section_time_key_pos)
                        y = pd.DataFrame(section_askPrice_key_pos)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_askPrice_pos_hat = np.array(lm.predict(X)).flatten()
                        askPrice_total_pct_chg_pos = np.divide(np.subtract(section_askPrice_pos_hat[-1], section_askPrice_pos_hat[0]), section_askPrice_pos_hat[0])
                        askPrice_avg_roc_pos = np.nanmean((np.diff(section_askPrice_pos_hat)/section_askPrice_pos_hat[:-1])/np.diff(section_time_key_pos))
                        askPrice_r2_pos = lm.score(X,y)
                    else:
                        askPrice_total_pct_chg_pos = np.nan
                        askPrice_avg_roc_pos = np.nan
                        askPrice_r2_pos = np.nan

                    if section_bidPrice_key_pos.size > 0:
                        X = pd.DataFrame(section_time_key_pos)
                        y = pd.DataFrame(section_bidPrice_key_pos)
                        y =  Imputer().fit_transform(y)
                        model_lm = lm.fit(X,y)
                        section_bidPrice_pos_hat = np.array(lm.predict(X)).flatten()
                        bidPrice_total_pct_chg_pos = np.divide(np.subtract(section_bidPrice_pos_hat[-1], section_bidPrice_pos_hat[0]), section_bidPrice_pos_hat[0])
                        bidPrice_avg_roc_pos = np.nanmean((np.diff(section_bidPrice_pos_hat)/section_bidPrice_pos_hat[:-1])/np.diff(section_time_key_pos))
                        bidPrice_r2_pos = lm.score(X,y)
                    else:
                        bidPrice_total_pct_chg_pos = np.nan
                        bidPrice_avg_roc_pos = np.nan
                        bidPrice_r2_pos = np.nan





                    final_output.update(dict(sec_1_avg_askPrice = avg_askPrice))                              
                    final_output.update(dict(sec_1_avg_bidPrice = avg_bidPrice)) 
                    final_output.update(dict(sec_1_current_askSize = current_askSize))                              
                    final_output.update(dict(sec_1_current_bidSize = current_bidSize))   
                    final_output.update(dict(sec_1_avg_askSize = avg_askSize))                              
                    final_output.update(dict(sec_1_avg_bidSize = avg_bidSize))                                   
                    final_output.update(dict(sec_1_mf_ask_total_pct_chg_neg = mf_ask_total_pct_chg_neg))                                                                         
                    final_output.update(dict(sec_1_mf_ask_avg_roc_neg = mf_ask_avg_roc_neg))                                                                         
                    final_output.update(dict(sec_1_mf_ask_r2_neg = mf_ask_r2_neg))                                                                         
                    final_output.update(dict(sec_1_mf_bid_total_pct_chg_neg = mf_bid_total_pct_chg_neg))                                                                         
                    final_output.update(dict(sec_1_mf_bid_avg_roc_neg = mf_bid_avg_roc_neg))                                                                         
                    final_output.update(dict(sec_1_mf_bid_r2_neg = mf_bid_r2_neg))                                                                         
                    final_output.update(dict(sec_1_ba_spread_total_pct_chg_neg = ba_spread_total_pct_chg_neg))                                                                         
                    final_output.update(dict(sec_1_ba_spread_r2_neg = ba_spread_r2_neg))                                                                       
                    final_output.update(dict(sec_1_askPrice_total_pct_chg_neg = askPrice_total_pct_chg_neg))                                                                         
                    final_output.update(dict(sec_1_askPrice_avg_roc_neg = askPrice_avg_roc_neg))                                                                        
                    final_output.update(dict(sec_1_askPrice_r2_neg = askPrice_r2_neg))                                                                         
                    final_output.update(dict(sec_1_bidPrice_total_pct_chg_neg = bidPrice_total_pct_chg_neg))                                                                         
                    final_output.update(dict(sec_1_bidPrice_avg_roc_neg = bidPrice_avg_roc_neg))                                                                         
                    final_output.update(dict(sec_1_bidPrice_r2_neg = bidPrice_r2_neg))                                                                     
                    final_output.update(dict(sec_1_mf_ask_total_pct_chg_pos = mf_ask_total_pct_chg_pos))                                     
                    final_output.update(dict(sec_1_mf_ask_avg_roc_pos = mf_ask_avg_roc_pos))                                     
                    final_output.update(dict(sec_1_mf_ask_r2_pos = mf_ask_r2_pos))                                     
                    final_output.update(dict(sec_1_mf_bid_total_pct_chg_pos = mf_bid_total_pct_chg_pos))                                     
                    final_output.update(dict(sec_1_mf_bid_avg_roc_pos = mf_bid_avg_roc_pos))                                                                         
                    final_output.update(dict(sec_1_mf_bid_r2_pos = mf_bid_r2_pos))                                                                         
                    final_output.update(dict(sec_1_ba_spread_total_pct_chg_pos = ba_spread_total_pct_chg_pos))                                                                         
                    final_output.update(dict(sec_1_ba_spread_r2_pos = ba_spread_r2_pos))                                                                
                    final_output.update(dict(sec_1_askPrice_total_pct_chg_pos = askPrice_total_pct_chg_pos))                                                                     
                    final_output.update(dict(sec_1_askPrice_avg_roc_pos = askPrice_avg_roc_pos))                                                                      
                    final_output.update(dict(sec_1_askPrice_r2_pos = askPrice_r2_pos))                                  
                    final_output.update(dict(sec_1_bidPrice_total_pct_chg_pos = bidPrice_total_pct_chg_pos))                                                                       
                    final_output.update(dict(sec_1_bidPrice_avg_roc_pos = bidPrice_avg_roc_pos))                                                                       
                    final_output.update(dict(sec_1_bidPrice_r2_pos = bidPrice_r2_pos))
                    final_output.update(dict(ts = None))
                    final_output.update(dict(final_y = None))
            
        return final_output

    message = request.get_json().get('message', '')
    element = message
    element = element.split(', [')
    Time_1 = element[0].split('[')[2].split(']')[:-1][0].split(', ')
    Symbol_1 = element[1].split(']')[0].split(', ')
    Price_1 = element[2].split(']')[0].split(', ')
    Volume_1 = element[3].split(']')[0].split(', ')
    askPrice_1 = element[4].split(']')[0].split(', ')
    bidPrice_1 = element[5].split(']')[0].split(', ')
    askSize_1 = element[6].split(']')[0].split(', ')
    bidSize_1 = element[7].split(']')[0].split(', ')

    element_pd = pd.DataFrame({'Time': Time_1, 'Symbol': Symbol_1, 'Price': Price_1, 'Volume': Volume_1, 'askPrice': askPrice_1, 'bidPrice': bidPrice_1, 'askSize': askSize_1, 'bidSize': bidSize_1})
    element_pd = element_pd[['Time', 'Symbol', 'Price', 'Volume', 'askPrice', 'bidPrice', 'askSize', 'bidSize']]

    final_output = {}
    symbol = str(element_pd['Symbol'][0])
    zero_crossings_0_total = np.array([])
    zero_crossings_0_total_time = np.array([])


    
    def takeLastTime(elem):
        elem = elem['Time'].apply(str)
        elem = str(elem[0][-22:-6])
        epoch = datetime.utcfromtimestamp(0)
        dt = datetime.combine(datetime.utcnow().date(), datetime.strptime(elem, '%H:%M:%S.%f').time())
        epoch_milli = float((dt - epoch).total_seconds() * 1000.0)
        return epoch_milli

    def dt2epoch(value):
        value = float(value)/1000000
        return value

    element_pd['Time'] = element_pd['Time'].apply(dt2epoch)
        
    element_pd.set_index('Time', inplace=True, drop=False)

    price_pd = element_pd.iloc[:, :4]
    #price_pd.drop_duplicates(keep='first', inplace=True)
    price_pd['Time'] = pd.to_datetime(price_pd['Time'], unit='ms', utc=True)
    price_pd['Price'] = price_pd.Price.astype(float)
    price_pd['Volume'] = price_pd.Volume.astype(float)
    price_pd.set_index('Time', inplace=True)
    price_pd.dropna(inplace=True)
    values = price_pd.index.duplicated(keep='first').astype(float)
    values[values==0] = np.NaN
    missings = np.isnan(values)
    cumsum = np.cumsum(~missings)
    diff = np.diff(np.concatenate(([0.], cumsum[missings])))
    values[missings] = -diff
    serialized_datetime = price_pd.index + np.cumsum(values).astype(np.timedelta64)
    price_pd.index = serialized_datetime
    price_pd.sort_index(inplace=True)
    price_pd[~price_pd.index.duplicated(keep='first')]
    Returned_None = 'invalid trade'
    return_1 = jsonify({'message':Returned_None})
    if price_pd.empty is False:
        time_sym_pd = element_pd.iloc[:,:1]
        quotes_side = element_pd.iloc[:, 4:]
        quotes_side = pd.concat([time_sym_pd, quotes_side], axis=1)
        #quotes_side.drop_duplicates(keep='first', inplace=True)
        quotes_side['Time'] = pd.to_datetime(quotes_side['Time'], unit='ms', utc=True)
        quotes_side['askPrice'] = quotes_side.askPrice.astype(float)
        quotes_side['bidPrice'] = quotes_side.bidPrice.astype(float)
        quotes_side['askSize'] = quotes_side.askSize.astype(float)
        quotes_side['bidSize'] = quotes_side.bidSize.astype(float)
        quotes_side.set_index('Time', inplace=True)
        quotes_side.dropna(inplace=True)
        values = quotes_side.index.duplicated(keep='first').astype(float)
        values[values==0] = np.nan
        missings = np.isnan(values)
        cumsum = np.cumsum(~missings)
        diff = np.diff(np.concatenate(([0.], cumsum[missings])))
        values[missings] = -diff
        serialized_datetime = quotes_side.index + np.cumsum(values).astype(np.timedelta64)
        quotes_side.index = serialized_datetime
        quotes_side.sort_index(inplace=True)
        quotes_side[~quotes_side.index.duplicated(keep='first')]

        section_price = price_pd['Price'].values
        section_time = price_pd.index.values.astype(np.float64)
        section_time = section_time * 0.000000000000000001
        section_vol = price_pd['Volume'].values
        model_pyearth = Earth(smooth=True, allow_missing=True)
        model_pyearth.fit(section_time, section_price)
        section_price_hat = model_pyearth.predict(section_time)
        section_der = model_pyearth.predict_deriv(section_time).astype(np.longdouble)
        section_der = np.concatenate([np.array(l) for l in section_der])
        section_der = np.concatenate([np.array(l) for l in section_der])
        section_der_2_diff = np.diff(section_der).astype(np.longdouble)
        section_time_2_diff = np.diff(section_time).astype(np.longdouble)
        section_der_2 = np.divide(section_der_2_diff, section_time_2_diff).astype(np.longdouble)
        zero_crossings = np.where(np.diff(np.sign(section_der)))[0]
        if zero_crossings.size > 0:
            if section_der[zero_crossings[-1]+1] > 0:
                
                #if l_total_mf >= minimum_money_flow and (section_price_hat[-1]-section_price_hat[zero_crossings[-1]])/section_price_hat[zero_crossings[-1]] <= .0001:
                find_curve_entry = np.where(section_der[int(zero_crossings[-1]):] >= 1)[0]
                if find_curve_entry.size > 1:
                    find_curve_entry = find_curve_entry[0]
                if find_curve_entry.size == 1: 
                    ba_spread_curve = quotes_side.iloc[int(zero_crossings[-1]):,:]
                    avg_ba_spread_curve = np.nanmean(np.divide(np.subtract(ba_spread_curve.askPrice.astype(float), ba_spread_curve.bidPrice.astype(float)), ba_spread_curve.askPrice.astype(float)))
                    if avg_ba_spread_curve <= .02:
                        final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model_pyearth, quotes_side)
                        #if section_price[-1] >= (section_price_hat[int(zero_crossings[-1]):][find_curve_entry]*(float(final_output.get('sec_1_residual_max'))*-1))+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]:
                            
                        #if float(final_output.get('sec_1_residual_max'))*2.5 <= .09:
                        keys_arr = np.array(['symbol', 'avg_return_40_mins', 'avg_chg_in_avg_return', 'return_minus_5', 'return_minus_4', 'return_minus_3', 'return_minus_2', 'lr_all_day_time_passed', 'lr_all_day_pct_chg', 'lr_all_day_r2', 'sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1', 'sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2', 'sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3', 'sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq', 'sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section', 'sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section', 'sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section', 'sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg', 'sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg', 'sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima', 'sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect', 'sec_1_residual_max', 'sec_1_residual_mean', 'sec_1_current_unix_time', 'sec_1_ms_since_open', 'total_days', 'sec_1_current_price', 'sec_1_current_price_hat_r', 'sec_1_avg_price', 'sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section', 'sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section', 'sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section', 'sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section', 'sec_1_price_pct_chg_neg', 'sec_1_price_pct_chg_pos', 'sec_1_lr_price_total_pct_chg_neg', 'sec_1_lr_price_avg_roc_neg', 'sec_1_lr_price_r2_neg', 'sec_1_lr_price_total_pct_chg_pos', 'sec_1_lr_price_avg_roc_pos', 'sec_1_lr_price_r2_pos', 'sec_1_vol_total_pct_chg_neg', 'sec_1_vol_total_pct_chg_pos', 'sec_1_vol_r2_neg', 'sec_1_vol_r2_pos', 'sec_1_mf_price_r2_neg', 'sec_1_mf_price_r2_pos', 'sec_1_avg_vol', 'sec_1_avg_askPrice', 'sec_1_avg_bidPrice', 'sec_1_current_askSize', 'sec_1_current_bidSize', 'sec_1_avg_askSize', 'sec_1_avg_bidSize', 'sec_1_mf_ask_total_pct_chg_neg', 'sec_1_mf_ask_avg_roc_neg', 'sec_1_mf_ask_r2_neg', 'sec_1_mf_bid_total_pct_chg_neg', 'sec_1_mf_bid_avg_roc_neg', 'sec_1_mf_bid_r2_neg', 'sec_1_ba_spread_total_pct_chg_neg', 'sec_1_ba_spread_r2_neg', 'sec_1_askPrice_total_pct_chg_neg', 'sec_1_askPrice_avg_roc_neg', 'sec_1_askPrice_r2_neg', 'sec_1_bidPrice_total_pct_chg_neg', 'sec_1_bidPrice_avg_roc_neg', 'sec_1_bidPrice_r2_neg', 'sec_1_mf_ask_total_pct_chg_pos', 'sec_1_mf_ask_avg_roc_pos', 'sec_1_mf_ask_r2_pos', 'sec_1_mf_bid_total_pct_chg_pos', 'sec_1_mf_bid_avg_roc_pos', 'sec_1_mf_bid_r2_pos', 'sec_1_ba_spread_total_pct_chg_pos', 'sec_1_ba_spread_r2_pos', 'sec_1_askPrice_total_pct_chg_pos', 'sec_1_askPrice_avg_roc_pos', 'sec_1_askPrice_r2_pos', 'sec_1_bidPrice_total_pct_chg_pos', 'sec_1_bidPrice_avg_roc_pos', 'sec_1_bidPrice_r2_pos', 'ts', 'final_y'])
            
                        final_output_first = {each_key: final_output.get(each_key) for each_key in keys_arr}
                        final_output = {k: None if isinstance(v,float) == True and np.isinf(v) == True or isinstance(v, float) == True and v <= -9223372036854775 or isinstance(v,float) == True and np.isnan(v) == True else v for k, v in final_output_first.items()}
                        h2o.init(ip="localhost", start_h2o=True)
                        final_output = pd.DataFrame(final_output, index=[0])
                        final_output.fillna(value=pd.np.nan, inplace=True)
                        final_output = final_output[keys_arr.tolist()]
                        final_output_h2o_frame = h2o.H2OFrame(final_output, column_names=keys_arr.tolist(), na_strings=['None', 'Nan', 'Na', 'nan'])
                        model_path = 'gs://your-bucket-name/StackedEnsemble_BestOfFamily_AutoML_1'
                        saved_model = h2o.load_model(model_path)
                        final_y_pred = saved_model.predict(final_output_h2o_frame)
                        final_y_pred = final_y_pred.as_data_frame()
                        final_y_pred = final_y_pred.max().values
                        final_y_pred = float(final_y_pred[0])
                        final_y_price = (final_y_pred*section_price_hat[int(zero_crossings[-1]):][find_curve_entry])+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]
                        current_final_y_pred = ((final_y_price - section_price[-1]) / section_price[-1])
                        h2o.cluster().shutdown()
                        
                        #if current_final_y_pred >= .05 and current_final_y_pred/float(final_output.get('sec_1_residual_max')) >= 6.5 and 350000 >= l_total_mf/(current_final_y_pred*100) >= 5000:
                        #if current_final_y_pred > 0:
                        liquidity_curve = price_pd.iloc[int(zero_crossings[-1]):,:]
                        time_curve = liquidity_curve.index.values.astype(np.float64)
                        time_curve_seconds = np.divide(time_curve, 1000000000)
                        total_seconds = (time_curve_seconds[-1]-time_curve_seconds[0])
                        l_total_mf = np.sum(np.multiply(liquidity_curve['Volume'].values, liquidity_curve['Price'].values))
                        avg_money_per_sec = float(l_total_mf/total_seconds)
                        avg_r_2 = np.nanmean(np.array([float(final_output.get('sec_1_residual_max_per_section')),float(final_output.get('sec_2_residual_max_per_section')),float(final_output.get('sec_3_residual_max_per_section')),float(final_output.get('sec_4_residual_max_per_section')),float(final_output.get('sec_5_residual_max_per_section')),float(final_output.get('sec_6_residual_max_per_section')),float(final_output.get('sec_7_residual_max_per_section')),float(final_output.get('sec_8_residual_max_per_section')),float(final_output.get('sec_9_residual_max_per_section')),float(final_output.get('sec_10_residual_max_per_section'))]))
                        df = [str('BUY'), str(np.multiply(section_time[-1],1000000000000)), str(datetime.utcnow().replace(tzinfo=pytz.utc)), symbol, str(avg_money_per_sec), str(section_price[-1]), str('rm'), str(final_output.get('sec_1_residual_max')), str('fyp'), str(final_y_pred), str('cfyp'), str(current_final_y_pred), str(section_time[int(zero_crossings[-1]):][find_curve_entry]), str(section_price_hat[int(zero_crossings[-1]):][find_curve_entry]), str(section_time[-1]), str(section_price_hat[-1]), str(section_price[-1]), str(avg_ba_spread_curve), str(section_price_hat[zero_crossings[-1]]), str(avg_r_2), str(ba_spread_curve.askPrice.astype(float).values[-1]), str(ba_spread_curve.bidPrice.astype(float).values[-1])]
                        df = ' '.join(df)
                        return_1 = jsonify({'message':df})
                        #else:
                        #    return_1 = jsonify({'message':Returned_None})
                    else:
                        return_1 = jsonify({'message':Returned_None})
                    #else:
                        #return_1 = jsonify({'message':Returned_None})
                    #else:
                        #return_1 = jsonify({'message':Returned_None})
                elif find_curve_entry.size == 0:
                    check_if_inflect = np.where(section_der_2[int(zero_crossings[-1]):] < 0)[0]
                    if check_if_inflect.size > 1:
                        check_if_inflect = check_if_inflect[0]
                    if check_if_inflect.size == 1:
                        find_curve_entry = check_if_inflect
                        ba_spread_curve = quotes_side.iloc[int(zero_crossings[-1]):,:]
                        avg_ba_spread_curve = np.nanmean(np.divide(np.subtract(ba_spread_curve.askPrice.astype(float), ba_spread_curve.bidPrice.astype(float)), ba_spread_curve.askPrice.astype(float)))
                        if avg_ba_spread_curve <= .02:
                            final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model_pyearth, quotes_side)
                            #if section_price[-1] >= (section_price_hat[int(zero_crossings[-1]):][find_curve_entry]*(float(final_output.get('sec_1_residual_max'))*-1))+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]:
                                
                            #if float(final_output.get('sec_1_residual_max'))*2.5 <= .09:
                            keys_arr = np.array(['symbol', 'avg_return_40_mins', 'avg_chg_in_avg_return', 'return_minus_5', 'return_minus_4', 'return_minus_3', 'return_minus_2', 'lr_all_day_time_passed', 'lr_all_day_pct_chg', 'lr_all_day_r2', 'sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1', 'sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2', 'sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3', 'sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq', 'sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section', 'sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section', 'sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section', 'sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg', 'sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg', 'sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima', 'sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect', 'sec_1_residual_max', 'sec_1_residual_mean', 'sec_1_current_unix_time', 'sec_1_ms_since_open', 'total_days', 'sec_1_current_price', 'sec_1_current_price_hat_r', 'sec_1_avg_price', 'sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section', 'sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section', 'sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section', 'sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section', 'sec_1_price_pct_chg_neg', 'sec_1_price_pct_chg_pos', 'sec_1_lr_price_total_pct_chg_neg', 'sec_1_lr_price_avg_roc_neg', 'sec_1_lr_price_r2_neg', 'sec_1_lr_price_total_pct_chg_pos', 'sec_1_lr_price_avg_roc_pos', 'sec_1_lr_price_r2_pos', 'sec_1_vol_total_pct_chg_neg', 'sec_1_vol_total_pct_chg_pos', 'sec_1_vol_r2_neg', 'sec_1_vol_r2_pos', 'sec_1_mf_price_r2_neg', 'sec_1_mf_price_r2_pos', 'sec_1_avg_vol', 'sec_1_avg_askPrice', 'sec_1_avg_bidPrice', 'sec_1_current_askSize', 'sec_1_current_bidSize', 'sec_1_avg_askSize', 'sec_1_avg_bidSize', 'sec_1_mf_ask_total_pct_chg_neg', 'sec_1_mf_ask_avg_roc_neg', 'sec_1_mf_ask_r2_neg', 'sec_1_mf_bid_total_pct_chg_neg', 'sec_1_mf_bid_avg_roc_neg', 'sec_1_mf_bid_r2_neg', 'sec_1_ba_spread_total_pct_chg_neg', 'sec_1_ba_spread_r2_neg', 'sec_1_askPrice_total_pct_chg_neg', 'sec_1_askPrice_avg_roc_neg', 'sec_1_askPrice_r2_neg', 'sec_1_bidPrice_total_pct_chg_neg', 'sec_1_bidPrice_avg_roc_neg', 'sec_1_bidPrice_r2_neg', 'sec_1_mf_ask_total_pct_chg_pos', 'sec_1_mf_ask_avg_roc_pos', 'sec_1_mf_ask_r2_pos', 'sec_1_mf_bid_total_pct_chg_pos', 'sec_1_mf_bid_avg_roc_pos', 'sec_1_mf_bid_r2_pos', 'sec_1_ba_spread_total_pct_chg_pos', 'sec_1_ba_spread_r2_pos', 'sec_1_askPrice_total_pct_chg_pos', 'sec_1_askPrice_avg_roc_pos', 'sec_1_askPrice_r2_pos', 'sec_1_bidPrice_total_pct_chg_pos', 'sec_1_bidPrice_avg_roc_pos', 'sec_1_bidPrice_r2_pos', 'ts', 'final_y'])
                
                            final_output_first = {each_key: final_output.get(each_key) for each_key in keys_arr}
                            final_output = {k: None if isinstance(v,float) == True and np.isinf(v) == True or isinstance(v, float) == True and v <= -9223372036854775 or isinstance(v,float) == True and np.isnan(v) == True else v for k, v in final_output_first.items()}
                            h2o.init(ip="localhost", start_h2o=True)
                            final_output = pd.DataFrame(final_output, index=[0])
                            final_output.fillna(value=pd.np.nan, inplace=True)
                            final_output = final_output[keys_arr.tolist()]
                            final_output_h2o_frame = h2o.H2OFrame(final_output, column_names=keys_arr.tolist(), na_strings=['None', 'Nan', 'Na', 'nan'])
                            model_path = 'gs://your-bucket-name/StackedEnsemble_BestOfFamily_AutoML_1'
                            saved_model = h2o.load_model(model_path)
                            final_y_pred = saved_model.predict(final_output_h2o_frame)
                            final_y_pred = final_y_pred.as_data_frame()
                            final_y_pred = final_y_pred.max().values
                            final_y_pred = float(final_y_pred[0])
                            final_y_price = (final_y_pred*section_price_hat[int(zero_crossings[-1]):][find_curve_entry])+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]
                            current_final_y_pred = ((final_y_price - section_price[-1]) / section_price[-1])
                            h2o.cluster().shutdown()
                            
                            #if current_final_y_pred >= .05 and current_final_y_pred/float(final_output.get('sec_1_residual_max')) >= 6.5 and 350000 >= l_total_mf/(current_final_y_pred*100) >= 5000:
                            #if current_final_y_pred > 0:
                            liquidity_curve = price_pd.iloc[int(zero_crossings[-1]):,:]
                            time_curve = liquidity_curve.index.values.astype(np.float64)
                            time_curve_seconds = np.divide(time_curve, 1000000000)
                            total_seconds = (time_curve_seconds[-1]-time_curve_seconds[0])
                            l_total_mf = np.sum(np.multiply(liquidity_curve['Volume'].values, liquidity_curve['Price'].values))
                            avg_money_per_sec = float(l_total_mf/total_seconds)
                            avg_r_2 = np.nanmean(np.array([float(final_output.get('sec_1_residual_max_per_section')),float(final_output.get('sec_2_residual_max_per_section')),float(final_output.get('sec_3_residual_max_per_section')),float(final_output.get('sec_4_residual_max_per_section')),float(final_output.get('sec_5_residual_max_per_section')),float(final_output.get('sec_6_residual_max_per_section')),float(final_output.get('sec_7_residual_max_per_section')),float(final_output.get('sec_8_residual_max_per_section')),float(final_output.get('sec_9_residual_max_per_section')),float(final_output.get('sec_10_residual_max_per_section'))]))
                            df = [str('BUY'), str(np.multiply(section_time[-1],1000000000000)), str(datetime.utcnow().replace(tzinfo=pytz.utc)), symbol, str(avg_money_per_sec), str(section_price[-1]), str('rm'), str(final_output.get('sec_1_residual_max')), str('fyp'), str(final_y_pred), str('cfyp'), str(current_final_y_pred), str(section_time[int(zero_crossings[-1]):][find_curve_entry]), str(section_price_hat[int(zero_crossings[-1]):][find_curve_entry]), str(section_time[-1]), str(section_price_hat[-1]), str(section_price[-1]), str(avg_ba_spread_curve), str(section_price_hat[zero_crossings[-1]]), str(avg_r_2), str(ba_spread_curve.askPrice.astype(float).values[-1]), str(ba_spread_curve.bidPrice.astype(float).values[-1])]
                            df = ' '.join(df)
                            return_1 = jsonify({'message':df})
                            #else:
                            #    return_1 = jsonify({'message':Returned_None})
                        else:
                            return_1 = jsonify({'message':Returned_None})
                        #else:
                            #return_1 = jsonify({'message':Returned_None})
                    else:
                        return_1 = jsonify({'message':Returned_None})
                else:
                    return_1 = jsonify({'message':Returned_None})
            
            elif section_der[zero_crossings[-1]+1] < 0:

                find_curve_entry = np.where(section_der[int(zero_crossings[-1]):] <= -1)[0]
                if find_curve_entry.size > 1:
                    find_curve_entry = find_curve_entry[0]
                if find_curve_entry.size == 1: 
                    ba_spread_curve = quotes_side.iloc[int(zero_crossings[-1]):,:]
                    avg_ba_spread_curve = np.nanmean(np.divide(np.subtract(ba_spread_curve.askPrice.astype(float), ba_spread_curve.bidPrice.astype(float)), ba_spread_curve.askPrice.astype(float)))
                    if avg_ba_spread_curve <= .02:
                        final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model_pyearth, quotes_side)
                        #if section_price[-1] >= (section_price_hat[int(zero_crossings[-1]):][find_curve_entry]*(float(final_output.get('sec_1_residual_max'))*-1))+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]:
                            
                        #if float(final_output.get('sec_1_residual_max'))*2.5 <= .09:
                        keys_arr = np.array(['symbol', 'avg_return_40_mins', 'avg_chg_in_avg_return', 'return_minus_5', 'return_minus_4', 'return_minus_3', 'return_minus_2', 'lr_all_day_time_passed', 'lr_all_day_pct_chg', 'lr_all_day_r2', 'sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1', 'sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2', 'sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3', 'sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq', 'sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section', 'sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section', 'sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section', 'sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg', 'sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg', 'sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima', 'sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect', 'sec_1_residual_max', 'sec_1_residual_mean', 'sec_1_current_unix_time', 'sec_1_ms_since_open', 'total_days', 'sec_1_current_price', 'sec_1_current_price_hat_r', 'sec_1_avg_price', 'sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section', 'sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section', 'sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section', 'sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section', 'sec_1_price_pct_chg_neg', 'sec_1_price_pct_chg_pos', 'sec_1_lr_price_total_pct_chg_neg', 'sec_1_lr_price_avg_roc_neg', 'sec_1_lr_price_r2_neg', 'sec_1_lr_price_total_pct_chg_pos', 'sec_1_lr_price_avg_roc_pos', 'sec_1_lr_price_r2_pos', 'sec_1_vol_total_pct_chg_neg', 'sec_1_vol_total_pct_chg_pos', 'sec_1_vol_r2_neg', 'sec_1_vol_r2_pos', 'sec_1_mf_price_r2_neg', 'sec_1_mf_price_r2_pos', 'sec_1_avg_vol', 'sec_1_avg_askPrice', 'sec_1_avg_bidPrice', 'sec_1_current_askSize', 'sec_1_current_bidSize', 'sec_1_avg_askSize', 'sec_1_avg_bidSize', 'sec_1_mf_ask_total_pct_chg_neg', 'sec_1_mf_ask_avg_roc_neg', 'sec_1_mf_ask_r2_neg', 'sec_1_mf_bid_total_pct_chg_neg', 'sec_1_mf_bid_avg_roc_neg', 'sec_1_mf_bid_r2_neg', 'sec_1_ba_spread_total_pct_chg_neg', 'sec_1_ba_spread_r2_neg', 'sec_1_askPrice_total_pct_chg_neg', 'sec_1_askPrice_avg_roc_neg', 'sec_1_askPrice_r2_neg', 'sec_1_bidPrice_total_pct_chg_neg', 'sec_1_bidPrice_avg_roc_neg', 'sec_1_bidPrice_r2_neg', 'sec_1_mf_ask_total_pct_chg_pos', 'sec_1_mf_ask_avg_roc_pos', 'sec_1_mf_ask_r2_pos', 'sec_1_mf_bid_total_pct_chg_pos', 'sec_1_mf_bid_avg_roc_pos', 'sec_1_mf_bid_r2_pos', 'sec_1_ba_spread_total_pct_chg_pos', 'sec_1_ba_spread_r2_pos', 'sec_1_askPrice_total_pct_chg_pos', 'sec_1_askPrice_avg_roc_pos', 'sec_1_askPrice_r2_pos', 'sec_1_bidPrice_total_pct_chg_pos', 'sec_1_bidPrice_avg_roc_pos', 'sec_1_bidPrice_r2_pos', 'ts', 'final_y'])
            
                        final_output_first = {each_key: final_output.get(each_key) for each_key in keys_arr}
                        final_output = {k: None if isinstance(v,float) == True and np.isinf(v) == True or isinstance(v, float) == True and v <= -9223372036854775 or isinstance(v,float) == True and np.isnan(v) == True else v for k, v in final_output_first.items()}
                        h2o.init(ip="localhost", start_h2o=True)
                        final_output = pd.DataFrame(final_output, index=[0])
                        final_output.fillna(value=pd.np.nan, inplace=True)
                        final_output = final_output[keys_arr.tolist()]
                        final_output_h2o_frame = h2o.H2OFrame(final_output, column_names=keys_arr.tolist(), na_strings=['None', 'Nan', 'Na', 'nan'])
                        model_path = 'gs://your-bucket-name/StackedEnsemble_BestOfFamily_AutoML_1'
                        saved_model = h2o.load_model(model_path)
                        final_y_pred = saved_model.predict(final_output_h2o_frame)
                        final_y_pred = final_y_pred.as_data_frame()
                        final_y_pred = final_y_pred.max().values
                        final_y_pred = float(final_y_pred[0])
                        final_y_price = (final_y_pred*section_price_hat[int(zero_crossings[-1]):][find_curve_entry])+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]
                        current_final_y_pred = ((final_y_price - section_price[-1]) / section_price[-1])
                        h2o.cluster().shutdown()
                        
                        #if current_final_y_pred >= .05 and current_final_y_pred/float(final_output.get('sec_1_residual_max')) >= 6.5 and 350000 >= l_total_mf/(current_final_y_pred*100) >= 5000:
                        #if current_final_y_pred < 0:
                        liquidity_curve = price_pd.iloc[int(zero_crossings[-1]):,:]
                        time_curve = liquidity_curve.index.values.astype(np.float64)
                        time_curve_seconds = np.divide(time_curve, 1000000000)
                        total_seconds = (time_curve_seconds[-1]-time_curve_seconds[0])
                        l_total_mf = np.sum(np.multiply(liquidity_curve['Volume'].values, liquidity_curve['Price'].values))
                        avg_money_per_sec = float(l_total_mf/total_seconds)
                        avg_r_2 = np.nanmean(np.array([float(final_output.get('sec_1_residual_max_per_section')),float(final_output.get('sec_2_residual_max_per_section')),float(final_output.get('sec_3_residual_max_per_section')),float(final_output.get('sec_4_residual_max_per_section')),float(final_output.get('sec_5_residual_max_per_section')),float(final_output.get('sec_6_residual_max_per_section')),float(final_output.get('sec_7_residual_max_per_section')),float(final_output.get('sec_8_residual_max_per_section')),float(final_output.get('sec_9_residual_max_per_section')),float(final_output.get('sec_10_residual_max_per_section'))]))
                        df = [str('SELL'), str(np.multiply(section_time[-1],1000000000000)), str(datetime.utcnow().replace(tzinfo=pytz.utc)), symbol, str(avg_money_per_sec), str(section_price[-1]), str('rm'), str(final_output.get('sec_1_residual_max')), str('fyp'), str(final_y_pred), str('cfyp'), str(current_final_y_pred), str(section_time[int(zero_crossings[-1]):][find_curve_entry]), str(section_price_hat[int(zero_crossings[-1]):][find_curve_entry]), str(section_time[-1]), str(section_price_hat[-1]), str(section_price[-1]), str(avg_ba_spread_curve), str(section_price_hat[zero_crossings[-1]]), str(avg_r_2), str(ba_spread_curve.askPrice.astype(float).values[-1]), str(ba_spread_curve.bidPrice.astype(float).values[-1])]
                        df = ' '.join(df)
                        return_1 = jsonify({'message':df})
                        #else:
                        #    return_1 = jsonify({'message':Returned_None})
                    else:
                        return_1 = jsonify({'message':Returned_None})
                    #else:
                        #return_1 = jsonify({'message':Returned_None})
                    #else:
                        #return_1 = jsonify({'message':Returned_None})
                elif find_curve_entry.size == 0:
                    check_if_inflect = np.where(section_der_2[int(zero_crossings[-1]):] > 0)[0]
                    if check_if_inflect.size > 1:
                        check_if_inflect = check_if_inflect[0]
                    if check_if_inflect.size == 1:
                        find_curve_entry = check_if_inflect
                        ba_spread_curve = quotes_side.iloc[int(zero_crossings[-1]):,:]
                        avg_ba_spread_curve = np.nanmean(np.divide(np.subtract(ba_spread_curve.askPrice.astype(float), ba_spread_curve.bidPrice.astype(float)), ba_spread_curve.askPrice.astype(float)))
                        if avg_ba_spread_curve <= .02:
                            final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model_pyearth, quotes_side)
                            #if section_price[-1] >= (section_price_hat[int(zero_crossings[-1]):][find_curve_entry]*(float(final_output.get('sec_1_residual_max'))*-1))+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]:
                                
                            #if float(final_output.get('sec_1_residual_max'))*2.5 <= .09:
                            keys_arr = np.array(['symbol', 'avg_return_40_mins', 'avg_chg_in_avg_return', 'return_minus_5', 'return_minus_4', 'return_minus_3', 'return_minus_2', 'lr_all_day_time_passed', 'lr_all_day_pct_chg', 'lr_all_day_r2', 'sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1', 'sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2', 'sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3', 'sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq', 'sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section', 'sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section', 'sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section', 'sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg', 'sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg', 'sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima', 'sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect', 'sec_1_residual_max', 'sec_1_residual_mean', 'sec_1_current_unix_time', 'sec_1_ms_since_open', 'total_days', 'sec_1_current_price', 'sec_1_current_price_hat_r', 'sec_1_avg_price', 'sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section', 'sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section', 'sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section', 'sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section', 'sec_1_price_pct_chg_neg', 'sec_1_price_pct_chg_pos', 'sec_1_lr_price_total_pct_chg_neg', 'sec_1_lr_price_avg_roc_neg', 'sec_1_lr_price_r2_neg', 'sec_1_lr_price_total_pct_chg_pos', 'sec_1_lr_price_avg_roc_pos', 'sec_1_lr_price_r2_pos', 'sec_1_vol_total_pct_chg_neg', 'sec_1_vol_total_pct_chg_pos', 'sec_1_vol_r2_neg', 'sec_1_vol_r2_pos', 'sec_1_mf_price_r2_neg', 'sec_1_mf_price_r2_pos', 'sec_1_avg_vol', 'sec_1_avg_askPrice', 'sec_1_avg_bidPrice', 'sec_1_current_askSize', 'sec_1_current_bidSize', 'sec_1_avg_askSize', 'sec_1_avg_bidSize', 'sec_1_mf_ask_total_pct_chg_neg', 'sec_1_mf_ask_avg_roc_neg', 'sec_1_mf_ask_r2_neg', 'sec_1_mf_bid_total_pct_chg_neg', 'sec_1_mf_bid_avg_roc_neg', 'sec_1_mf_bid_r2_neg', 'sec_1_ba_spread_total_pct_chg_neg', 'sec_1_ba_spread_r2_neg', 'sec_1_askPrice_total_pct_chg_neg', 'sec_1_askPrice_avg_roc_neg', 'sec_1_askPrice_r2_neg', 'sec_1_bidPrice_total_pct_chg_neg', 'sec_1_bidPrice_avg_roc_neg', 'sec_1_bidPrice_r2_neg', 'sec_1_mf_ask_total_pct_chg_pos', 'sec_1_mf_ask_avg_roc_pos', 'sec_1_mf_ask_r2_pos', 'sec_1_mf_bid_total_pct_chg_pos', 'sec_1_mf_bid_avg_roc_pos', 'sec_1_mf_bid_r2_pos', 'sec_1_ba_spread_total_pct_chg_pos', 'sec_1_ba_spread_r2_pos', 'sec_1_askPrice_total_pct_chg_pos', 'sec_1_askPrice_avg_roc_pos', 'sec_1_askPrice_r2_pos', 'sec_1_bidPrice_total_pct_chg_pos', 'sec_1_bidPrice_avg_roc_pos', 'sec_1_bidPrice_r2_pos', 'ts', 'final_y'])
                
                            final_output_first = {each_key: final_output.get(each_key) for each_key in keys_arr}
                            final_output = {k: None if isinstance(v,float) == True and np.isinf(v) == True or isinstance(v, float) == True and v <= -9223372036854775 or isinstance(v,float) == True and np.isnan(v) == True else v for k, v in final_output_first.items()}
                            h2o.init(ip="localhost", start_h2o=True)
                            final_output = pd.DataFrame(final_output, index=[0])
                            final_output.fillna(value=pd.np.nan, inplace=True)
                            final_output = final_output[keys_arr.tolist()]
                            final_output_h2o_frame = h2o.H2OFrame(final_output, column_names=keys_arr.tolist(), na_strings=['None', 'Nan', 'Na', 'nan'])
                            model_path = 'gs://your-bucket-name/StackedEnsemble_BestOfFamily_AutoML_1'
                            saved_model = h2o.load_model(model_path)
                            final_y_pred = saved_model.predict(final_output_h2o_frame)
                            final_y_pred = final_y_pred.as_data_frame()
                            final_y_pred = final_y_pred.max().values
                            final_y_pred = float(final_y_pred[0])
                            final_y_price = (final_y_pred*section_price_hat[int(zero_crossings[-1]):][find_curve_entry])+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]
                            current_final_y_pred = ((final_y_price - section_price[-1]) / section_price[-1])
                            h2o.cluster().shutdown()
                            
                            #if current_final_y_pred >= .05 and current_final_y_pred/float(final_output.get('sec_1_residual_max')) >= 6.5 and 350000 >= l_total_mf/(current_final_y_pred*100) >= 5000:
                            #if current_final_y_pred < 0:
                            liquidity_curve = price_pd.iloc[int(zero_crossings[-1]):,:]
                            time_curve = liquidity_curve.index.values.astype(np.float64)
                            time_curve_seconds = np.divide(time_curve, 1000000000)
                            total_seconds = (time_curve_seconds[-1]-time_curve_seconds[0])
                            l_total_mf = np.sum(np.multiply(liquidity_curve['Volume'].values, liquidity_curve['Price'].values))
                            avg_money_per_sec = float(l_total_mf/total_seconds)
                            avg_r_2 = np.nanmean(np.array([float(final_output.get('sec_1_residual_max_per_section')),float(final_output.get('sec_2_residual_max_per_section')),float(final_output.get('sec_3_residual_max_per_section')),float(final_output.get('sec_4_residual_max_per_section')),float(final_output.get('sec_5_residual_max_per_section')),float(final_output.get('sec_6_residual_max_per_section')),float(final_output.get('sec_7_residual_max_per_section')),float(final_output.get('sec_8_residual_max_per_section')),float(final_output.get('sec_9_residual_max_per_section')),float(final_output.get('sec_10_residual_max_per_section'))]))
                            df = [str('SELL'), str(np.multiply(section_time[-1],1000000000000)), str(datetime.utcnow().replace(tzinfo=pytz.utc)), symbol, str(avg_money_per_sec), str(section_price[-1]), str('rm'), str(final_output.get('sec_1_residual_max')), str('fyp'), str(final_y_pred), str('cfyp'), str(current_final_y_pred), str(section_time[int(zero_crossings[-1]):][find_curve_entry]), str(section_price_hat[int(zero_crossings[-1]):][find_curve_entry]), str(section_time[-1]), str(section_price_hat[-1]), str(section_price[-1]), str(avg_ba_spread_curve), str(section_price_hat[zero_crossings[-1]]), str(avg_r_2), str(ba_spread_curve.askPrice.astype(float).values[-1]), str(ba_spread_curve.bidPrice.astype(float).values[-1])]
                            df = ' '.join(df)
                            return_1 = jsonify({'message':df})
                            #else:
                            #    return_1 = jsonify({'message':Returned_None})
                        else:
                            return_1 = jsonify({'message':Returned_None})
                        #else:
                            #return_1 = jsonify({'message':Returned_None})
                    else:
                        return_1 = jsonify({'message':Returned_None})
                else:
                    return_1 = jsonify({'message':Returned_None})
            else:
                return_1 = jsonify({'message':Returned_None})

        #elif zero_crossings.size == 0 and datetime.utcnow().replace(hour=13, minute=35, tzinfo=pytz.utc) > datetime.utcnow().replace(tzinfo=pytz.utc):
        elif zero_crossings.size == 0:
            if section_der[0+1] > 0:
                zero_crossings = np.insert(zero_crossings, 0, 0)

                #if l_total_mf >= minimum_money_flow and (section_price_hat[-1]-section_price_hat[zero_crossings[-1]])/section_price_hat[zero_crossings[-1]] <= .0001:
                find_curve_entry = np.where(section_der[int(zero_crossings[-1]):] >= 1)[0]
                if find_curve_entry.size > 1:
                    find_curve_entry = find_curve_entry[0]
                if find_curve_entry.size == 1: 
                    ba_spread_curve = quotes_side.iloc[int(zero_crossings[-1]):,:]
                    ba_spread_arr = np.divide(np.subtract(ba_spread_curve.askPrice.astype(float), ba_spread_curve.bidPrice.astype(float)), ba_spread_curve.askPrice.astype(float))
                    avg_ba_spread_curve = np.nanmean(ba_spread_arr)
                    if avg_ba_spread_curve <= .02:
                        final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model_pyearth, quotes_side)
                        #if section_price[-1] >= (section_price_hat[int(zero_crossings[-1]):][find_curve_entry]*(float(final_output.get('sec_1_residual_max'))*-1))+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]:
                            
                        #if float(final_output.get('sec_1_residual_max'))*2.5 <= .09:
                        keys_arr = np.array(['symbol', 'avg_return_40_mins', 'avg_chg_in_avg_return', 'return_minus_5', 'return_minus_4', 'return_minus_3', 'return_minus_2', 'lr_all_day_time_passed', 'lr_all_day_pct_chg', 'lr_all_day_r2', 'sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1', 'sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2', 'sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3', 'sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq', 'sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section', 'sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section', 'sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section', 'sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg', 'sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg', 'sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima', 'sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect', 'sec_1_residual_max', 'sec_1_residual_mean', 'sec_1_current_unix_time', 'sec_1_ms_since_open', 'total_days', 'sec_1_current_price', 'sec_1_current_price_hat_r', 'sec_1_avg_price', 'sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section', 'sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section', 'sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section', 'sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section', 'sec_1_price_pct_chg_neg', 'sec_1_price_pct_chg_pos', 'sec_1_lr_price_total_pct_chg_neg', 'sec_1_lr_price_avg_roc_neg', 'sec_1_lr_price_r2_neg', 'sec_1_lr_price_total_pct_chg_pos', 'sec_1_lr_price_avg_roc_pos', 'sec_1_lr_price_r2_pos', 'sec_1_vol_total_pct_chg_neg', 'sec_1_vol_total_pct_chg_pos', 'sec_1_vol_r2_neg', 'sec_1_vol_r2_pos', 'sec_1_mf_price_r2_neg', 'sec_1_mf_price_r2_pos', 'sec_1_avg_vol', 'sec_1_avg_askPrice', 'sec_1_avg_bidPrice', 'sec_1_current_askSize', 'sec_1_current_bidSize', 'sec_1_avg_askSize', 'sec_1_avg_bidSize', 'sec_1_mf_ask_total_pct_chg_neg', 'sec_1_mf_ask_avg_roc_neg', 'sec_1_mf_ask_r2_neg', 'sec_1_mf_bid_total_pct_chg_neg', 'sec_1_mf_bid_avg_roc_neg', 'sec_1_mf_bid_r2_neg', 'sec_1_ba_spread_total_pct_chg_neg', 'sec_1_ba_spread_r2_neg', 'sec_1_askPrice_total_pct_chg_neg', 'sec_1_askPrice_avg_roc_neg', 'sec_1_askPrice_r2_neg', 'sec_1_bidPrice_total_pct_chg_neg', 'sec_1_bidPrice_avg_roc_neg', 'sec_1_bidPrice_r2_neg', 'sec_1_mf_ask_total_pct_chg_pos', 'sec_1_mf_ask_avg_roc_pos', 'sec_1_mf_ask_r2_pos', 'sec_1_mf_bid_total_pct_chg_pos', 'sec_1_mf_bid_avg_roc_pos', 'sec_1_mf_bid_r2_pos', 'sec_1_ba_spread_total_pct_chg_pos', 'sec_1_ba_spread_r2_pos', 'sec_1_askPrice_total_pct_chg_pos', 'sec_1_askPrice_avg_roc_pos', 'sec_1_askPrice_r2_pos', 'sec_1_bidPrice_total_pct_chg_pos', 'sec_1_bidPrice_avg_roc_pos', 'sec_1_bidPrice_r2_pos', 'ts', 'final_y'])
            
                        final_output_first = {each_key: final_output.get(each_key) for each_key in keys_arr}
                        final_output = {k: None if isinstance(v,float) == True and np.isinf(v) == True or isinstance(v, float) == True and v <= -9223372036854775 or isinstance(v,float) == True and np.isnan(v) == True else v for k, v in final_output_first.items()}
                        h2o.init(ip="localhost", start_h2o=True)
                        final_output = pd.DataFrame(final_output, index=[0])
                        final_output.fillna(value=pd.np.nan, inplace=True)
                        final_output = final_output[keys_arr.tolist()]
                        final_output_h2o_frame = h2o.H2OFrame(final_output, column_names=keys_arr.tolist(), na_strings=['None', 'Nan', 'Na', 'nan'])
                        model_path = 'gs://your-bucket-name/StackedEnsemble_BestOfFamily_AutoML_1'
                        saved_model = h2o.load_model(model_path)
                        final_y_pred = saved_model.predict(final_output_h2o_frame)
                        final_y_pred = final_y_pred.as_data_frame()
                        final_y_pred = final_y_pred.max().values
                        final_y_pred = float(final_y_pred[0])
                        final_y_price = (final_y_pred*section_price_hat[int(zero_crossings[-1]):][find_curve_entry])+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]
                        current_final_y_pred = ((final_y_price - section_price[-1]) / section_price[-1])
                        h2o.cluster().shutdown()
                        
                        #if current_final_y_pred >= .05 and current_final_y_pred/float(final_output.get('sec_1_residual_max')) >= 6.5 and 350000 >= l_total_mf/(current_final_y_pred*100) >= 5000:
                        #if current_final_y_pred > 0:
                        liquidity_curve = price_pd.iloc[int(zero_crossings[-1]):,:]
                        time_curve = liquidity_curve.index.values.astype(np.float64)
                        time_curve_seconds = np.divide(time_curve, 1000000000)
                        total_seconds = (time_curve_seconds[-1]-time_curve_seconds[0])
                        l_total_mf = np.sum(np.multiply(liquidity_curve['Volume'].values, liquidity_curve['Price'].values))
                        avg_money_per_sec = float(l_total_mf/total_seconds)
                        avg_r_2 = np.nanmean(np.array([float(final_output.get('sec_1_residual_max_per_section')),float(final_output.get('sec_2_residual_max_per_section')),float(final_output.get('sec_3_residual_max_per_section')),float(final_output.get('sec_4_residual_max_per_section')),float(final_output.get('sec_5_residual_max_per_section')),float(final_output.get('sec_6_residual_max_per_section')),float(final_output.get('sec_7_residual_max_per_section')),float(final_output.get('sec_8_residual_max_per_section')),float(final_output.get('sec_9_residual_max_per_section')),float(final_output.get('sec_10_residual_max_per_section'))]))
                        df = [str('BUY'), str(np.multiply(section_time[-1],1000000000000)), str(datetime.utcnow().replace(tzinfo=pytz.utc)), symbol, str(avg_money_per_sec), str(section_price[-1]), str('rm'), str(final_output.get('sec_1_residual_max')), str('fyp'), str(final_y_pred), str('cfyp'), str(current_final_y_pred), str(section_time[int(zero_crossings[-1]):][find_curve_entry]), str(section_price_hat[int(zero_crossings[-1]):][find_curve_entry]), str(section_time[-1]), str(section_price_hat[-1]), str(section_price[-1]), str(avg_ba_spread_curve), str(section_price_hat[zero_crossings[-1]]), str(avg_r_2), str(ba_spread_curve.askPrice.astype(float).values[-1]), str(ba_spread_curve.bidPrice.astype(float).values[-1])]
                        df = ' '.join(df)
                        return_1 = jsonify({'message':df})
                        #else:
                        #    return_1 = jsonify({'message':Returned_None})
                    else:
                        return_1 = jsonify({'message':Returned_None})
                    #else:
                        #return_1 = jsonify({'message':Returned_None})
                    #else:
                        #return_1 = jsonify({'message':Returned_None})
                elif find_curve_entry.size == 0:
                    check_if_inflect = np.where(section_der_2[int(zero_crossings[-1]):] < 0)[0]
                    if check_if_inflect.size > 1:
                        check_if_inflect = check_if_inflect[0]
                    if check_if_inflect.size == 1:
                        find_curve_entry = check_if_inflect
                        ba_spread_curve = quotes_side.iloc[int(zero_crossings[-1]):,:]
                        avg_ba_spread_curve = np.nanmean(np.divide(np.subtract(ba_spread_curve.askPrice.astype(float), ba_spread_curve.bidPrice.astype(float)), ba_spread_curve.askPrice.astype(float)))
                        if avg_ba_spread_curve <= .02:
                            final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model_pyearth, quotes_side)
                            #if section_price[-1] >= (section_price_hat[int(zero_crossings[-1]):][find_curve_entry]*(float(final_output.get('sec_1_residual_max'))*-1))+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]:
                                
                            #if float(final_output.get('sec_1_residual_max'))*2.5 <= .09:
                            keys_arr = np.array(['symbol', 'avg_return_40_mins', 'avg_chg_in_avg_return', 'return_minus_5', 'return_minus_4', 'return_minus_3', 'return_minus_2', 'lr_all_day_time_passed', 'lr_all_day_pct_chg', 'lr_all_day_r2', 'sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1', 'sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2', 'sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3', 'sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq', 'sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section', 'sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section', 'sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section', 'sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg', 'sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg', 'sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima', 'sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect', 'sec_1_residual_max', 'sec_1_residual_mean', 'sec_1_current_unix_time', 'sec_1_ms_since_open', 'total_days', 'sec_1_current_price', 'sec_1_current_price_hat_r', 'sec_1_avg_price', 'sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section', 'sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section', 'sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section', 'sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section', 'sec_1_price_pct_chg_neg', 'sec_1_price_pct_chg_pos', 'sec_1_lr_price_total_pct_chg_neg', 'sec_1_lr_price_avg_roc_neg', 'sec_1_lr_price_r2_neg', 'sec_1_lr_price_total_pct_chg_pos', 'sec_1_lr_price_avg_roc_pos', 'sec_1_lr_price_r2_pos', 'sec_1_vol_total_pct_chg_neg', 'sec_1_vol_total_pct_chg_pos', 'sec_1_vol_r2_neg', 'sec_1_vol_r2_pos', 'sec_1_mf_price_r2_neg', 'sec_1_mf_price_r2_pos', 'sec_1_avg_vol', 'sec_1_avg_askPrice', 'sec_1_avg_bidPrice', 'sec_1_current_askSize', 'sec_1_current_bidSize', 'sec_1_avg_askSize', 'sec_1_avg_bidSize', 'sec_1_mf_ask_total_pct_chg_neg', 'sec_1_mf_ask_avg_roc_neg', 'sec_1_mf_ask_r2_neg', 'sec_1_mf_bid_total_pct_chg_neg', 'sec_1_mf_bid_avg_roc_neg', 'sec_1_mf_bid_r2_neg', 'sec_1_ba_spread_total_pct_chg_neg', 'sec_1_ba_spread_r2_neg', 'sec_1_askPrice_total_pct_chg_neg', 'sec_1_askPrice_avg_roc_neg', 'sec_1_askPrice_r2_neg', 'sec_1_bidPrice_total_pct_chg_neg', 'sec_1_bidPrice_avg_roc_neg', 'sec_1_bidPrice_r2_neg', 'sec_1_mf_ask_total_pct_chg_pos', 'sec_1_mf_ask_avg_roc_pos', 'sec_1_mf_ask_r2_pos', 'sec_1_mf_bid_total_pct_chg_pos', 'sec_1_mf_bid_avg_roc_pos', 'sec_1_mf_bid_r2_pos', 'sec_1_ba_spread_total_pct_chg_pos', 'sec_1_ba_spread_r2_pos', 'sec_1_askPrice_total_pct_chg_pos', 'sec_1_askPrice_avg_roc_pos', 'sec_1_askPrice_r2_pos', 'sec_1_bidPrice_total_pct_chg_pos', 'sec_1_bidPrice_avg_roc_pos', 'sec_1_bidPrice_r2_pos', 'ts', 'final_y'])
                
                            final_output_first = {each_key: final_output.get(each_key) for each_key in keys_arr}
                            final_output = {k: None if isinstance(v,float) == True and np.isinf(v) == True or isinstance(v, float) == True and v <= -9223372036854775 or isinstance(v,float) == True and np.isnan(v) == True else v for k, v in final_output_first.items()}
                            h2o.init(ip="localhost", start_h2o=True)
                            final_output = pd.DataFrame(final_output, index=[0])
                            final_output.fillna(value=pd.np.nan, inplace=True)
                            final_output = final_output[keys_arr.tolist()]
                            final_output_h2o_frame = h2o.H2OFrame(final_output, column_names=keys_arr.tolist(), na_strings=['None', 'Nan', 'Na', 'nan'])
                            model_path = 'gs://your-bucket-name/StackedEnsemble_BestOfFamily_AutoML_1'
                            saved_model = h2o.load_model(model_path)
                            final_y_pred = saved_model.predict(final_output_h2o_frame)
                            final_y_pred = final_y_pred.as_data_frame()
                            final_y_pred = final_y_pred.max().values
                            final_y_pred = float(final_y_pred[0])
                            final_y_price = (final_y_pred*section_price_hat[int(zero_crossings[-1]):][find_curve_entry])+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]
                            current_final_y_pred = ((final_y_price - section_price[-1]) / section_price[-1])
                            h2o.cluster().shutdown()
                            
                            #if current_final_y_pred >= .05 and current_final_y_pred/float(final_output.get('sec_1_residual_max')) >= 6.5 and 350000 >= l_total_mf/(current_final_y_pred*100) >= 5000:
                            #if current_final_y_pred > 0:
                            liquidity_curve = price_pd.iloc[int(zero_crossings[-1]):,:]
                            time_curve = liquidity_curve.index.values.astype(np.float64)
                            time_curve_seconds = np.divide(time_curve, 1000000000)
                            total_seconds = (time_curve_seconds[-1]-time_curve_seconds[0])
                            l_total_mf = np.sum(np.multiply(liquidity_curve['Volume'].values, liquidity_curve['Price'].values))
                            avg_money_per_sec = float(l_total_mf/total_seconds)
                            avg_r_2 = np.nanmean(np.array([float(final_output.get('sec_1_residual_max_per_section')),float(final_output.get('sec_2_residual_max_per_section')),float(final_output.get('sec_3_residual_max_per_section')),float(final_output.get('sec_4_residual_max_per_section')),float(final_output.get('sec_5_residual_max_per_section')),float(final_output.get('sec_6_residual_max_per_section')),float(final_output.get('sec_7_residual_max_per_section')),float(final_output.get('sec_8_residual_max_per_section')),float(final_output.get('sec_9_residual_max_per_section')),float(final_output.get('sec_10_residual_max_per_section'))]))
                            df = [str('BUY'), str(np.multiply(section_time[-1],1000000000000)), str(datetime.utcnow().replace(tzinfo=pytz.utc)), symbol, str(avg_money_per_sec), str(section_price[-1]), str('rm'), str(final_output.get('sec_1_residual_max')), str('fyp'), str(final_y_pred), str('cfyp'), str(current_final_y_pred), str(section_time[int(zero_crossings[-1]):][find_curve_entry]), str(section_price_hat[int(zero_crossings[-1]):][find_curve_entry]), str(section_time[-1]), str(section_price_hat[-1]), str(section_price[-1]), str(avg_ba_spread_curve), str(section_price_hat[zero_crossings[-1]]), str(avg_r_2), str(ba_spread_curve.askPrice.astype(float).values[-1]), str(ba_spread_curve.bidPrice.astype(float).values[-1])]
                            df = ' '.join(df)
                            return_1 = jsonify({'message':df})
                            #else:
                            #    return_1 = jsonify({'message':Returned_None})
                        else:
                            return_1 = jsonify({'message':Returned_None})
                        #else:
                            #return_1 = jsonify({'message':Returned_None})
                    else:
                        return_1 = jsonify({'message':Returned_None})
                else:
                    return_1 = jsonify({'message':Returned_None})





            elif section_der[0+1] < 0:
                zero_crossings = np.insert(zero_crossings, 0, 0)
                find_curve_entry = np.where(section_der[int(zero_crossings[-1]):] <= -1)[0]
                if find_curve_entry.size > 1:
                    find_curve_entry = find_curve_entry[0]
                if find_curve_entry.size == 1: 
                    ba_spread_curve = quotes_side.iloc[int(zero_crossings[-1]):,:]
                    avg_ba_spread_curve = np.nanmean(np.divide(np.subtract(ba_spread_curve.askPrice.astype(float), ba_spread_curve.bidPrice.astype(float)), ba_spread_curve.askPrice.astype(float)))
                    if avg_ba_spread_curve <= .02:
                        final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model_pyearth, quotes_side)
                        #if section_price[-1] >= (section_price_hat[int(zero_crossings[-1]):][find_curve_entry]*(float(final_output.get('sec_1_residual_max'))*-1))+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]:
                            
                        #if float(final_output.get('sec_1_residual_max'))*2.5 <= .09:
                        keys_arr = np.array(['symbol', 'avg_return_40_mins', 'avg_chg_in_avg_return', 'return_minus_5', 'return_minus_4', 'return_minus_3', 'return_minus_2', 'lr_all_day_time_passed', 'lr_all_day_pct_chg', 'lr_all_day_r2', 'sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1', 'sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2', 'sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3', 'sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq', 'sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section', 'sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section', 'sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section', 'sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg', 'sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg', 'sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima', 'sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect', 'sec_1_residual_max', 'sec_1_residual_mean', 'sec_1_current_unix_time', 'sec_1_ms_since_open', 'total_days', 'sec_1_current_price', 'sec_1_current_price_hat_r', 'sec_1_avg_price', 'sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section', 'sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section', 'sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section', 'sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section', 'sec_1_price_pct_chg_neg', 'sec_1_price_pct_chg_pos', 'sec_1_lr_price_total_pct_chg_neg', 'sec_1_lr_price_avg_roc_neg', 'sec_1_lr_price_r2_neg', 'sec_1_lr_price_total_pct_chg_pos', 'sec_1_lr_price_avg_roc_pos', 'sec_1_lr_price_r2_pos', 'sec_1_vol_total_pct_chg_neg', 'sec_1_vol_total_pct_chg_pos', 'sec_1_vol_r2_neg', 'sec_1_vol_r2_pos', 'sec_1_mf_price_r2_neg', 'sec_1_mf_price_r2_pos', 'sec_1_avg_vol', 'sec_1_avg_askPrice', 'sec_1_avg_bidPrice', 'sec_1_current_askSize', 'sec_1_current_bidSize', 'sec_1_avg_askSize', 'sec_1_avg_bidSize', 'sec_1_mf_ask_total_pct_chg_neg', 'sec_1_mf_ask_avg_roc_neg', 'sec_1_mf_ask_r2_neg', 'sec_1_mf_bid_total_pct_chg_neg', 'sec_1_mf_bid_avg_roc_neg', 'sec_1_mf_bid_r2_neg', 'sec_1_ba_spread_total_pct_chg_neg', 'sec_1_ba_spread_r2_neg', 'sec_1_askPrice_total_pct_chg_neg', 'sec_1_askPrice_avg_roc_neg', 'sec_1_askPrice_r2_neg', 'sec_1_bidPrice_total_pct_chg_neg', 'sec_1_bidPrice_avg_roc_neg', 'sec_1_bidPrice_r2_neg', 'sec_1_mf_ask_total_pct_chg_pos', 'sec_1_mf_ask_avg_roc_pos', 'sec_1_mf_ask_r2_pos', 'sec_1_mf_bid_total_pct_chg_pos', 'sec_1_mf_bid_avg_roc_pos', 'sec_1_mf_bid_r2_pos', 'sec_1_ba_spread_total_pct_chg_pos', 'sec_1_ba_spread_r2_pos', 'sec_1_askPrice_total_pct_chg_pos', 'sec_1_askPrice_avg_roc_pos', 'sec_1_askPrice_r2_pos', 'sec_1_bidPrice_total_pct_chg_pos', 'sec_1_bidPrice_avg_roc_pos', 'sec_1_bidPrice_r2_pos', 'ts', 'final_y'])
            
                        final_output_first = {each_key: final_output.get(each_key) for each_key in keys_arr}
                        final_output = {k: None if isinstance(v,float) == True and np.isinf(v) == True or isinstance(v, float) == True and v <= -9223372036854775 or isinstance(v,float) == True and np.isnan(v) == True else v for k, v in final_output_first.items()}
                        h2o.init(ip="localhost", start_h2o=True)
                        final_output = pd.DataFrame(final_output, index=[0])
                        final_output.fillna(value=pd.np.nan, inplace=True)
                        final_output = final_output[keys_arr.tolist()]
                        final_output_h2o_frame = h2o.H2OFrame(final_output, column_names=keys_arr.tolist(), na_strings=['None', 'Nan', 'Na', 'nan'])
                        model_path = 'gs://your-bucket-name/StackedEnsemble_BestOfFamily_AutoML_1'
                        saved_model = h2o.load_model(model_path)
                        final_y_pred = saved_model.predict(final_output_h2o_frame)
                        final_y_pred = final_y_pred.as_data_frame()
                        final_y_pred = final_y_pred.max().values
                        final_y_pred = float(final_y_pred[0])
                        final_y_price = (final_y_pred*section_price_hat[int(zero_crossings[-1]):][find_curve_entry])+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]
                        current_final_y_pred = ((final_y_price - section_price[-1]) / section_price[-1])
                        h2o.cluster().shutdown()
                        
                        #if current_final_y_pred >= .05 and current_final_y_pred/float(final_output.get('sec_1_residual_max')) >= 6.5 and 350000 >= l_total_mf/(current_final_y_pred*100) >= 5000:
                        #if current_final_y_pred < 0:
                        liquidity_curve = price_pd.iloc[int(zero_crossings[-1]):,:]
                        time_curve = liquidity_curve.index.values.astype(np.float64)
                        time_curve_seconds = np.divide(time_curve, 1000000000)
                        total_seconds = (time_curve_seconds[-1]-time_curve_seconds[0])
                        l_total_mf = np.sum(np.multiply(liquidity_curve['Volume'].values, liquidity_curve['Price'].values))
                        avg_money_per_sec = float(l_total_mf/total_seconds)
                        avg_r_2 = np.nanmean(np.array([float(final_output.get('sec_1_residual_max_per_section')),float(final_output.get('sec_2_residual_max_per_section')),float(final_output.get('sec_3_residual_max_per_section')),float(final_output.get('sec_4_residual_max_per_section')),float(final_output.get('sec_5_residual_max_per_section')),float(final_output.get('sec_6_residual_max_per_section')),float(final_output.get('sec_7_residual_max_per_section')),float(final_output.get('sec_8_residual_max_per_section')),float(final_output.get('sec_9_residual_max_per_section')),float(final_output.get('sec_10_residual_max_per_section'))]))
                        df = [str('SELL'), str(np.multiply(section_time[-1],1000000000000)), str(datetime.utcnow().replace(tzinfo=pytz.utc)), symbol, str(avg_money_per_sec), str(section_price[-1]), str('rm'), str(final_output.get('sec_1_residual_max')), str('fyp'), str(final_y_pred), str('cfyp'), str(current_final_y_pred), str(section_time[int(zero_crossings[-1]):][find_curve_entry]), str(section_price_hat[int(zero_crossings[-1]):][find_curve_entry]), str(section_time[-1]), str(section_price_hat[-1]), str(section_price[-1]), str(avg_ba_spread_curve), str(section_price_hat[zero_crossings[-1]]), str(avg_r_2), str(ba_spread_curve.askPrice.astype(float).values[-1]), str(ba_spread_curve.bidPrice.astype(float).values[-1])]
                        df = ' '.join(df)
                        return_1 = jsonify({'message':df})
                        #else:
                        #    return_1 = jsonify({'message':Returned_None})
                    else:
                        return_1 = jsonify({'message':Returned_None})
                    #else:
                        #return_1 = jsonify({'message':Returned_None})
                    #else:
                        #return_1 = jsonify({'message':Returned_None})
                elif find_curve_entry.size == 0:
                    check_if_inflect = np.where(section_der_2[int(zero_crossings[-1]):] > 0)[0]
                    if check_if_inflect.size > 1:
                        check_if_inflect = check_if_inflect[0]
                    if check_if_inflect.size == 1:
                        find_curve_entry = check_if_inflect
                        ba_spread_curve = quotes_side.iloc[int(zero_crossings[-1]):,:]
                        avg_ba_spread_curve = np.nanmean(np.divide(np.subtract(ba_spread_curve.askPrice.astype(float), ba_spread_curve.bidPrice.astype(float)), ba_spread_curve.askPrice.astype(float)))
                        if avg_ba_spread_curve <= .02:
                            final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model_pyearth, quotes_side)
                            #if section_price[-1] >= (section_price_hat[int(zero_crossings[-1]):][find_curve_entry]*(float(final_output.get('sec_1_residual_max'))*-1))+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]:
                                
                            #if float(final_output.get('sec_1_residual_max'))*2.5 <= .09:
                            keys_arr = np.array(['symbol', 'avg_return_40_mins', 'avg_chg_in_avg_return', 'return_minus_5', 'return_minus_4', 'return_minus_3', 'return_minus_2', 'lr_all_day_time_passed', 'lr_all_day_pct_chg', 'lr_all_day_r2', 'sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1', 'sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2', 'sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3', 'sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq', 'sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section', 'sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section', 'sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section', 'sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg', 'sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg', 'sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima', 'sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect', 'sec_1_residual_max', 'sec_1_residual_mean', 'sec_1_current_unix_time', 'sec_1_ms_since_open', 'total_days', 'sec_1_current_price', 'sec_1_current_price_hat_r', 'sec_1_avg_price', 'sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section', 'sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section', 'sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section', 'sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section', 'sec_1_price_pct_chg_neg', 'sec_1_price_pct_chg_pos', 'sec_1_lr_price_total_pct_chg_neg', 'sec_1_lr_price_avg_roc_neg', 'sec_1_lr_price_r2_neg', 'sec_1_lr_price_total_pct_chg_pos', 'sec_1_lr_price_avg_roc_pos', 'sec_1_lr_price_r2_pos', 'sec_1_vol_total_pct_chg_neg', 'sec_1_vol_total_pct_chg_pos', 'sec_1_vol_r2_neg', 'sec_1_vol_r2_pos', 'sec_1_mf_price_r2_neg', 'sec_1_mf_price_r2_pos', 'sec_1_avg_vol', 'sec_1_avg_askPrice', 'sec_1_avg_bidPrice', 'sec_1_current_askSize', 'sec_1_current_bidSize', 'sec_1_avg_askSize', 'sec_1_avg_bidSize', 'sec_1_mf_ask_total_pct_chg_neg', 'sec_1_mf_ask_avg_roc_neg', 'sec_1_mf_ask_r2_neg', 'sec_1_mf_bid_total_pct_chg_neg', 'sec_1_mf_bid_avg_roc_neg', 'sec_1_mf_bid_r2_neg', 'sec_1_ba_spread_total_pct_chg_neg', 'sec_1_ba_spread_r2_neg', 'sec_1_askPrice_total_pct_chg_neg', 'sec_1_askPrice_avg_roc_neg', 'sec_1_askPrice_r2_neg', 'sec_1_bidPrice_total_pct_chg_neg', 'sec_1_bidPrice_avg_roc_neg', 'sec_1_bidPrice_r2_neg', 'sec_1_mf_ask_total_pct_chg_pos', 'sec_1_mf_ask_avg_roc_pos', 'sec_1_mf_ask_r2_pos', 'sec_1_mf_bid_total_pct_chg_pos', 'sec_1_mf_bid_avg_roc_pos', 'sec_1_mf_bid_r2_pos', 'sec_1_ba_spread_total_pct_chg_pos', 'sec_1_ba_spread_r2_pos', 'sec_1_askPrice_total_pct_chg_pos', 'sec_1_askPrice_avg_roc_pos', 'sec_1_askPrice_r2_pos', 'sec_1_bidPrice_total_pct_chg_pos', 'sec_1_bidPrice_avg_roc_pos', 'sec_1_bidPrice_r2_pos', 'ts', 'final_y'])
                
                            final_output_first = {each_key: final_output.get(each_key) for each_key in keys_arr}
                            final_output = {k: None if isinstance(v,float) == True and np.isinf(v) == True or isinstance(v, float) == True and v <= -9223372036854775 or isinstance(v,float) == True and np.isnan(v) == True else v for k, v in final_output_first.items()}
                            h2o.init(ip="localhost", start_h2o=True)
                            final_output = pd.DataFrame(final_output, index=[0])
                            final_output.fillna(value=pd.np.nan, inplace=True)
                            final_output = final_output[keys_arr.tolist()]
                            final_output_h2o_frame = h2o.H2OFrame(final_output, column_names=keys_arr.tolist(), na_strings=['None', 'Nan', 'Na', 'nan'])
                            model_path = 'gs://your-bucket-name/StackedEnsemble_BestOfFamily_AutoML_1'
                            saved_model = h2o.load_model(model_path)
                            final_y_pred = saved_model.predict(final_output_h2o_frame)
                            final_y_pred = final_y_pred.as_data_frame()
                            final_y_pred = final_y_pred.max().values
                            final_y_pred = float(final_y_pred[0])
                            final_y_price = (final_y_pred*section_price_hat[int(zero_crossings[-1]):][find_curve_entry])+section_price_hat[int(zero_crossings[-1]):][find_curve_entry]
                            current_final_y_pred = ((final_y_price - section_price[-1]) / section_price[-1])
                            h2o.cluster().shutdown()
                            
                            #if current_final_y_pred >= .05 and current_final_y_pred/float(final_output.get('sec_1_residual_max')) >= 6.5 and 350000 >= l_total_mf/(current_final_y_pred*100) >= 5000:
                            #if current_final_y_pred < 0:
                            liquidity_curve = price_pd.iloc[int(zero_crossings[-1]):,:]
                            time_curve = liquidity_curve.index.values.astype(np.float64)
                            time_curve_seconds = np.divide(time_curve, 1000000000)
                            total_seconds = (time_curve_seconds[-1]-time_curve_seconds[0])
                            l_total_mf = np.sum(np.multiply(liquidity_curve['Volume'].values, liquidity_curve['Price'].values))
                            avg_money_per_sec = float(l_total_mf/total_seconds)
                            avg_r_2 = np.nanmean(np.array([float(final_output.get('sec_1_residual_max_per_section')),float(final_output.get('sec_2_residual_max_per_section')),float(final_output.get('sec_3_residual_max_per_section')),float(final_output.get('sec_4_residual_max_per_section')),float(final_output.get('sec_5_residual_max_per_section')),float(final_output.get('sec_6_residual_max_per_section')),float(final_output.get('sec_7_residual_max_per_section')),float(final_output.get('sec_8_residual_max_per_section')),float(final_output.get('sec_9_residual_max_per_section')),float(final_output.get('sec_10_residual_max_per_section'))]))
                            df = [str('SELL'), str(np.multiply(section_time[-1],1000000000000)), str(datetime.utcnow().replace(tzinfo=pytz.utc)), symbol, str(avg_money_per_sec), str(section_price[-1]), str('rm'), str(final_output.get('sec_1_residual_max')), str('fyp'), str(final_y_pred), str('cfyp'), str(current_final_y_pred), str(section_time[int(zero_crossings[-1]):][find_curve_entry]), str(section_price_hat[int(zero_crossings[-1]):][find_curve_entry]), str(section_time[-1]), str(section_price_hat[-1]), str(section_price[-1]), str(avg_ba_spread_curve), str(section_price_hat[zero_crossings[-1]]), str(avg_r_2), str(ba_spread_curve.askPrice.astype(float).values[-1]), str(ba_spread_curve.bidPrice.astype(float).values[-1])]
                            df = ' '.join(df)
                            return_1 = jsonify({'message':df})
                            #else:
                            #    return_1 = jsonify({'message':Returned_None})
                        else:
                            return_1 = jsonify({'message':Returned_None})
                        #else:
                            #return_1 = jsonify({'message':Returned_None})
                    else:
                        return_1 = jsonify({'message':Returned_None})
                else:
                    return_1 = jsonify({'message':Returned_None})
            else:
                return_1 = jsonify({'message':Returned_None})

        else:
            return_1 = jsonify({'message':Returned_None})
                                
                    
    else:
        return_1 = jsonify({'message':Returned_None})

    return return_1


# [START endpoints_auth_info_backend]
def auth_info():
    """Retrieves the authenication information from Google Cloud Endpoints."""
    encoded_info = request.headers.get('X-Endpoint-API-UserInfo', None)

    if encoded_info:
        info_json = _base64_decode(encoded_info)
        user_info = json.loads(info_json)
    else:
        user_info = {'id': 'anonymous'}

    return jsonify(user_info)
# [START endpoints_auth_info_backend]


@app.route('/auth/info/googlejwt', methods=['GET'])
def auth_info_google_jwt():
    """Auth info with Google signed JWT."""
    return auth_info()


@app.route('/auth/info/googleidtoken', methods=['GET'])
def auth_info_google_id_token():
    """Auth info with Google ID token."""
    return auth_info()


@app.route('/auth/info/firebase', methods=['GET'])
@cross_origin(send_wildcard=True)
def auth_info_firebase():
    """Auth info with Firebase auth."""
    return auth_info()


@app.errorhandler(http_client.INTERNAL_SERVER_ERROR)
def unexpected_error(e):
    """Handle exceptions by returning swagger-compliant json."""
    logging.exception('An error occured while processing the request.')
    response = jsonify({
        'code': http_client.INTERNAL_SERVER_ERROR,
        'message': 'Exception: {}'.format(e)})
    response.status_code = http_client.INTERNAL_SERVER_ERROR
    return response


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)