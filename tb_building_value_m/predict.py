# This Python file uses the following encoding: utf-8

'''
楼宇通信价值预测_预测数据 @20180201 @yxm
应用时注意数据文件名及参数类型
'''

import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.externals import joblib
import os

print("TX_DAY:", sys.argv[1])

os.chdir('/home/hadoop/td_model/tb_building_value_m')  # 设定工作目录
pd.set_option('display.max_rows', None)      #设置全部输出，不含省略号

# 调用各区域函数进行预测
def applyModel(area):
    fileName = 'in_data/predict_' + area +TX_DAY+ '.txt'
    modelNme = 'pkl/rfModel_minSSQ_' + area + '.pkl'
    # print(fileName)
    # print(modelNme)
    predict_data_orgin = pd.read_table(fileName)
    predict_data = predict_data_orgin.drop(['CODE'],axis=1)

    print(len(predict_data))
    
    rf = joblib.load(modelNme)     # 读入模型
    predict_re = pd.DataFrame(rf.predict(predict_data))
    louyu_result=pd.concat([predict_data_orgin['CODE'],predict_re],axis=1)
    return louyu_result

# 应用预测
predict_cy = applyModel('dongxi')     #东西城
#print("predict of chaoyang: ", predict_cy)
predict_cy.to_csv('out_data/export_predict_dx_'+TX_DAY+'.dat',header=False,index=False, sep=',')

predict_cy = applyModel('chaoyang')     #朝阳区
#print("predict of chaoyang: ", predict_cy)
predict_cy.to_csv('out_data/export_predict_cy_'+TX_DAY+'.dat',header=False,index=False, sep=',')

predict_cy = applyModel('haidian')     #海淀区
#print("predict of chaoyang: ", predict_cy)
predict_cy.to_csv('out_data/export_predict_hd_'+TX_DAY+'.dat',header=False,index=False, sep=',')

predict_cy = applyModel('jiaoqu')     #郊区
#print("predict of chaoyang: ", predict_cy)
predict_cy.to_csv('out_data/export_predict_jq_'+TX_DAY+'.dat',header=False,index=False, sep=',')
