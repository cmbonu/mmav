import pandas as pd
import networkx as nx
from exceptions.data_source import FighterNotFoundException
from itertools import combinations

class FightsDataSource:
    
    #MASTER_DB = None
    #ROOSTER = None
    #NETWORK = None

    ATTRS = ['date','location','country','weight_class','side','Older','Taller','Rangier','Heavier',\
             'win_lose','opponent_Stance']
    BASE_ATTR = ['R_fighter','B_fighter','finish_details','finish','title_bout','weight_class','gender','empty_arena',\
            'date','location','country', 'no_of_rounds','finish_round','finish_round_time','total_fight_time_secs',\
            'Winner',]
    FIGHT_METRICS = ['td_attempted_bout','rev_bout','pass_bout','sub_attempts_bout','td_landed_bout','tot_str_attempted_bout',\
                    'tot_str_landed_bout','sig_str_attempted_bout','sig_str_landed_bout','kd_bout']
    OTHER_METRICS = ['age','odds','ev','current_lose_streak','current_win_streak','longest_win_streak','Height_cms','Stance',\
                        'Reach_cms','Weight_lbs','current_lose_streak','match_weightclass_rank']
    AGG_COLS = ['country','weight_class','side','Older','Taller','Rangier','Heavier','opponent_Stance']
          
    def __init__(self,file_path):
        self.MASTER_DB = self.__process_datafile(file_path)
        self.ROOSTER = self.__create_rooster()
        self.NETWORK = self.__create_graph_network()
        
    def __process_datafile(self,file_path):
        #Create Master Data
        dbfile = pd.read_csv(file_path)
        dbfile['finish_details'] = dbfile.apply(lambda x : x['finish'] if x['finish_details'] != x['finish_details']\
                                                    else x['finish_details'], axis=1)
        return dbfile
                
    def __create_rooster(self):
        #Create Rooster
        red_rooster = self.MASTER_DB[['R_fighter','weight_class','gender']].drop_duplicates()
        blue_rooster = self.MASTER_DB[['B_fighter','weight_class','gender']].drop_duplicates()

        columns = ['fighter','weight_class','gender']
        red_rooster.columns = columns
        blue_rooster.columns = columns

        rooster_df = pd.concat([blue_rooster,red_rooster],axis=0).drop_duplicates()
        rooster_all = [{'ID':fighter[0],'FIGHTER':fighter[1].fighter,'WEIGHTCLASS':fighter[1].weight_class,'GENDER':fighter[1].gender}\
                            for fighter in rooster_df.iterrows()]
        return rooster_all 

    def __create_graph_network(self):
        #Create Graph Network
        nx_nodes = self.unique_fighters()
        nx_network = nx.MultiGraph()
        nx_network.add_nodes_from(nx_nodes)
        for i, fight in self.MASTER_DB.iterrows():
            nx_network.add_edge(fight['R_fighter'],fight['B_fighter'])

        return nx_network

    def __calculate_comparisons(self,df):
        '''
        Summary : Compare fighter attributes and return as flags 
        '''
        df['Older'] = df.apply(lambda x : 'Y' if x['age'] >= x['opponent_age'] else 'N', axis=1)
        df['Taller'] = df.apply(lambda x : 'Y' if x['Height_cms'] >= x['opponent_Height_cms'] else 'N', axis=1)
        df['Rangier'] = df.apply(lambda x : 'Y' if x['Reach_cms'] >= x['opponent_Reach_cms'] else 'N', axis=1)
        df['Heavier'] = df.apply(lambda x : 'Y' if x['Weight_lbs'] >= x['opponent_Weight_lbs'] else 'N', axis=1)
        return df[FightsDataSource.ATTRS]

    def __find_fights_by_fighter_name(self,fighter_name):
        ff = self.MASTER_DB[self.BASE_ATTR].query(f'R_fighter =="{fighter_name}" or B_fighter =="{fighter_name}"').copy(deep=True)
        ff['side'] = ff.apply(lambda x : 'R' if x['R_fighter'] == fighter_name else 'B',axis=1)
        #ff['fighter_name'] = fighter_name
        ff_opp = ff.apply(lambda x : x['B_fighter'] if x['R_fighter'] == fighter_name else x['R_fighter'],axis=1)
        ff.insert(1,column='fighter_name',value=fighter_name)
        ff.insert(2,column='opponent',value=ff_opp)
        ff['win_lose'] = ff.apply(lambda x : 'W' if (x['side'] == 'R' and x['Winner'] == 'Red') or x['side'] == 'B' and x['Winner'] == 'Blue' else 'L',axis=1)
        ff.drop(['R_fighter','B_fighter','Winner'],axis=1,inplace=True)
        return ff

    def __materialize_fight_metric(self,fighter_name, metric, include_name=False): #losses
        cols = ['R_fighter','B_fighter',f'R_{metric}',f'B_{metric}']
        ff = self.MASTER_DB[cols].query(f'R_fighter =="{fighter_name}" or B_fighter =="{fighter_name}"').copy(deep=True)
        ff['fighter_name'] = fighter_name
        #ff['opponent'] = ff.apply(lambda x : x['B_fighter'] if x['R_fighter'] == fighter_name else x['R_fighter'],axis=1)
        ff['side'] = ff.apply(lambda x : 'R' if x['R_fighter'] == fighter_name else 'B',axis=1)
        ff[metric] = ff.apply(lambda x : x[f'R_{metric}'] if x['side'] == 'R' else x[f'B_{metric}'],axis=1 )
        ff[f'opponent_{metric}'] = ff.apply(lambda x : x[f'R_{metric}'] if x['side'] == 'B' else x[f'R_{metric}'],axis=1 )
        if include_name:
            return ff[['fighter_name',metric,f'opponent_{metric}']] #'fighter_name','opponent',
        else:
            return ff[[metric,f'opponent_{metric}']]

    def __inflate_fights_profile(self,fighter_name):
        inflated_profile = pd.concat([self.__find_fights_by_fighter_name(fighter_name)\
                            ,self.__materialize_fight_metric(fighter_name, 'Stance')\
                            ,self.__materialize_fight_metric(fighter_name, 'age')\
                            ,self.__materialize_fight_metric(fighter_name, 'Reach_cms')\
                            ,self.__materialize_fight_metric(fighter_name, 'Height_cms')\
                            ,self.__materialize_fight_metric(fighter_name, 'Weight_lbs')\
                            ],axis=1)
        return inflated_profile

    def __ufc_fights_records_deep_analysis(self,df,aggregate_cols,limit,win_rate_ascending_sort=False):
        #df = self.MASTER_DB
        aaa = []
        for i in range(1,limit+1):
            for comb in combinations(aggregate_cols,i):
                all_cols = list(comb)
                all_cols.extend(['date','win_lose'])

                group_cols = list(comb)
                group_cols.append('win_lose')
                aa = df[all_cols].groupby(group_cols).count().reset_index()
                aab = pd.pivot_table(aa,values='date',aggfunc='sum',columns='win_lose',fill_value=0,\
                        index=list(comb)).reset_index()
                
                if 'L' not in aab.columns:
                    aab['L'] = 0
                if 'W' not in aab.columns:
                    aab['W'] = 0
                
                aab['total'] = aab['L'] + aab['W']
                aab['win_rate'] = (aab['W']+1)/(aab['total']+2)
                aab['loss_rate'] = (aab['L']+1)/(aab['total']+2)
                
                agg_values = aab.apply(lambda x : ' , '.join([f'"{cc}":"{x[cc]}"' for cc in list(comb)]),axis=1)
                aab.insert(0,column='agg_cols',value='>>>'.join(list(comb)))
                aab.insert(1,column='agg_values',value=agg_values)
                aab.insert(2,column='col_count',value=len(comb))
                aab.drop(list(comb),axis=1,inplace=True)
                
                aaa.append(aab)
        return pd.concat(aaa,axis=0).sort_values(by='win_rate',ascending=win_rate_ascending_sort)

    def unique_fighters(self):
        unique_red_fighters = list(self.MASTER_DB['R_fighter'].unique())
        unique_blue_fighters = list(self.MASTER_DB['B_fighter'].unique())
        unique_red_fighters.extend(list(unique_blue_fighters))
        all_fighters = set(unique_red_fighters)
        return all_fighters

    def find_fighters_by_parameter(self, PARAM, value):
        """Find fighters in rooster by weight, gender and name

        Args:
            PARAM (String): Parameter to apply filter on possible values ['FIGHTER','WEIGHTCLASS','GENDER','ID']
            value (String): Filter value

        Returns:
            List : List of dictionaries of fighters meeting applied filter criteria
        """
        if PARAM == 'ID':
            value = int(value)
        return [x for x in self.ROOSTER if x[PARAM] == value]
        #return list(filter(lambda x : x[PARAM] == value,self.ROOSTER))
    
    def find_fighters_by_parameters(self, PARAM_LIST):
        """Find fighters in rooster by weight, gender and name

        Args:
            PARAM_LIST (List): List of Parameters to apply filter on possible values in the format [["KEY 1": "VALUE 1"],["KEY 2":"VALUE 2"]]
                               Valid Keys:
                                 1. FIGHTER
                                 2. WEIGHTCLASS
                                 3. GENDER
                                 4. ID

        Returns:
            List : List of dictionaries of fighters meeting applied filter criteria
        """
        lf_rooster = self.ROOSTER
        for param in PARAM_LIST:
            pname = param[0]
            pval = param[1]
            #lf_rooster = self.find_fighters_by_parameter(pname,pval)
            
            if pname == 'ID':
                pval = int(pval)
            #lf_rooster = list(filter(lambda x : x[pname] == pval,lf_rooster))
            lf_rooster = [x for x in lf_rooster if x[pname]==pval]

        if len(lf_rooster) == 0:
            raise FighterNotFoundException(f'No Fighter for defined parameter {PARAM_LIST}')

        lf_rooster.sort(key=lambda x : x['FIGHTER'])
        return lf_rooster

    def fetch_fighter_profile(self,fighter_id):
        fighter_profile = dict()
        fighter_name_list = [x for x in self.ROOSTER if x['ID'] == fighter_id]
        if len(fighter_name_list) < 1:
            raise FighterNotFoundException(f'Fighter with ID: {fighter_id} not found')
        
        fighter_name = fighter_name_list[0]['FIGHTER']

        fights = self.__inflate_fights_profile(fighter_name)
        fights_analysis = self.__calculate_comparisons(fights)
        
        fighter_profile['name'] = fighter_name
        fighter_profile['height'] = fights.iloc[0].Height_cms
        fighter_profile['reach'] = fights.iloc[0].Reach_cms
        fighter_profile['stance'] = fights.iloc[0].Stance
        fighter_profile['avg_fight_time'] = fights.total_fight_time_secs.mean()
        fighter_profile['no_of_fights'] = fights.shape[0]
        fighter_profile['wins'] = fights.query('win_lose == "W"').shape[0]
        fighter_profile['losses'] = fights.query('win_lose == "L"').shape[0]
        fighter_profile['win_rate'] = fighter_profile['wins']/fighter_profile['no_of_fights']
        
        summary = self.__ufc_fights_records_deep_analysis(fights_analysis,FightsDataSource.AGG_COLS,3,False)
        summary['pct_of_total'] = summary['total']/fights.shape[0]
        
        selection = summary
        #selection = summary.query(f"win_rate > {fighter_profile['win_rate']*1.1}").sort_values(by=['win_rate','col_count'],ascending=[False,True])
        #'analysis':fights_analysis.to_dict('records')
        return {'profile':fighter_profile, 'fights':fights.to_dict('records'), 'summary':selection.to_dict('records')}

    def find_shortest_paths(self,fighter_a,fighter_b):
        return list(nx.all_shortest_paths(self.NETWORK,fighter_a,fighter_b))