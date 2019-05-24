# -*- coding: utf-8 -*-
import pandas as pd

dff = pd.read_excel('/Users/xuegeng/Downloads/总的数据4（全有效）.xlsx')

#col_num = max(dff.catagories)
#fake_vars= map(lambda x:'var%s'%x,range(1,col_num))
#
#
#
#shop_ids = list(dff.shop_id)
#dfk = pd.DataFrame(columns = fake_vars,index = shop_ids,data=0)
#
#for id_ in shop_ids:
#
#    kind = dff[dff['shop_id']==id_]['catagories'].values[0]
#    if kind==61:print id_
#    if kind!=1:
#        dfk.ix[id_]['var%d'%kind] = 1
#    else:pass



#dfk.to_excel('/Users/xuegeng/Desktop/categories_fake_vars.xlsx')




#col_num = max(dff.region)
#fake_vars= map(lambda x:'var%s'%x,range(1,col_num))
#
#shop_ids = list(dff.shop_id)
#dfk = pd.DataFrame(columns = fake_vars,index = shop_ids,data=0)
#
#for id_ in shop_ids:
#
#    kind = dff[dff['shop_id']==id_]['region'].values[0]
#    if kind==61:print id_
#    if kind!=1:
#        dfk.ix[id_]['var%d'%kind] = 1
#    else:pass
#
#
#dfk.to_excel('/Users/xuegeng/Desktop/categories_fake_vars.xlsx')

