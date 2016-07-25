        
# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
# @ Author: Gustavo P. Avelar  (gpavelar)                      @
# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

''' IMPORT PACKAGES '''
import sys
# Dealing with json
import json
import simplejson
# Dealing with date and time
import datetime
import time
# Parsing command line parameters
import argparse
import re

# THIS CODE ENSURE THE CORRECT ENCODING (OPTIONAL)
reload(sys)
sys.setdefaultencoding('utf-8')

# THE ID's OF SELECTED USERS OF THE 10 MOST POPULATED CITIES OF BRAZIL
top_cities = ["bh", "bsb", "ctba", "forz", "man", "poal", "rec", "rj", "sp", "ssa"]

user_names_bh = ["TransitoBH","Bombeiros_MG","oficialbhtrans","cbnbhz","PRF191MG","transito98fm","blitzbh"]
ids_bh = [63459107,213705567,524349796,95958399,144834802,466779164,202750352]
count_bh = 0

user_names_bsb = ["fujadotransito","transitoBSB","metrobrasilia","leisecadf"]
ids_bsb = [124894540,138436041,282066682,63756700]	
count_bsb = 0

user_names_ctba = ["TransitoSetran","RodoviasParana","Ctbaaovivo","preparana","gpestradas","viapar","PRF191PR","transitocwb","CBNCuritiba","ecovia","plantao190"]
ids_ctba = [171502474,881194452,629596700,96828209,96129950,123154339,39251495,69037483,38413341,59857882,15493428]
count_ctba = 0

user_names_forz = ["LeiSecaFortal","porondefortal","fortalezaamc","CaronaFortal"]
ids_forz = [111358205,705767392555704320,747331110,119945005]
count_forz = 0

user_names_man = ["TransitoManaus"]#,Manaustrans]
ids_man = [54794044]
count_man = 0

user_names_poal = ["TransitoPOARS","transitozh","EPTC_POA","RadarBlitzPOA","PRF191RS"]
ids_poal = [551433766,125838023,80574781,227400370,71048313]
count_poal = 0

user_names_rec = ["jctransito","CTTU_Recife","cbntransito","transitorec_","transitolivrePE"]
ids_rec = [131191045,124291387,1700651635,132245890,281616484]
count_rec = 0

user_names_rj = ["LinhaAmarelaRJ","WazeTrafficRIo","OperacoesRio","transito_rjo","SuperVia_trens","LeiSecaRJ","InformeRJO","RadarCostaVerde","transitorj","odia24horas","transitoriorj","transitonaponte","cetrio_online","transitorjo","metro_rio"]
ids_rj = [188766141,3216708957,226409689,557644540,76395804,46152686,285443624,269832565,36859263,239472818,2737650336,108019366,41877324,40227784,49759770]
count_rj = 0

user_names_sp = ["transitoagorasp","sptransito","bandtransitosp","CETSP_,metrosp_oficial","sptrans_","CPTM_oficial","ecopistas","_dersp","mobilidadesampa","cptmnoticiando","SPTransNoticias","metronoticiando","Comando_SP","WazeTrafficSP","Linha1Azul_","linha1azul","L1Azul","Linha2_Verde","L2Verde","linha2verde","Linha3_Vermelha","L3Vermelha","linha3vermelha","Linha4_Amarela","L4Amarela_","linha4amarela","Linha5_Lilas","L5lilas","linha5lilas","Linha6_Laranja","L6laranja","Linha7_CPTM","linha7rubi","Linha8_CPTM","linha8diamante","Linha9_CPTM","linha9esmeralda","l9cptm","linha9_cptm","Linha10_CPTM","linha10turquegd","linha10cptm","Linha10Turquesa","Linha11_CPTM","linha11coral","Linha12_CPTM","linha12safiragd","Linha13_CPTM","linha13jade","linha13jadegd","Linha14Onix","linha14_onix","Linha15_Prata","linha15prata","l15prata","Linha16Violeta_","linha16violeta","Linha17_Ouro","linha17ouro","Linha18Bronze","Linha18Bronze_","Linha19Celeste_","linha19_celeste","Linha20Rosa_","Linha20Rosa","Linha21Roxa_","linha21roxa","Linha22Cobre_","Linha22Cobre","MetroObserva","cptmobserva","diariometrosp","Direto_do_Metro","metro_obs","Linhasrmsp","Direto_da_CPTM","diariodacptm","viatrolebus","UsuarioCPTM","transito"]
ids_sp = [127777676,74451624,27744369,339261594,75279848,407193525,75281850,59860902,988909723,2802535639,529288460,2871796583,2977489835,1330679324,3164364005,2909317673,2910067247,1957102418,2910163162,1957310742,2910074495,2910089813,1957575397,2910048615,2907437675,2288429389,1467108541,2910170194,1957640899,2910084377,2442634645,2523109171,2911216655,745435689915801601,2911251220,2910062331,2911191945,2910138706,2535541982,2911191945,2911194453,2910084141,1969141178,1002793183,2911258714,2462265049,2911198197,2910183928,2911200825,2231799298,2910167140,2426514847,2802823009,2908687293,2560256947,2151224654,2923810840,2802885794,2442458233,2150306511,2253354470,2923984984,2923968435,2482587994,2924056204,2442687720,2924018667,2802718812,2924060879,2802879204,2896568618,2896639921,1050923264,1963411399,260363646,1723819422,2384378864,273150222,95293870,136019766,9611162]
count_sp = 0

user_names_ssa = ["blitzemssa","transalvador1","transitossa","SSAtransito","transitoonline2","transito71","bolsba","transito_ssa","Blitzsalvador","transitoba093","PRFBAHIA","iBahia"]
ids_ssa = [113233147,1070774640,43350093,127872022,146919216,409247612,491440333,115908504,353505412,305228832,84942211,15850306]
count_ssa = 0

''' GETS THE COMMAND LINE PARAMETERS '''
def get_parameters():
    global args
    parser = argparse.ArgumentParser(description='This program deal with json tweets and retrieve the information about selected users.')
    ''' Arquivo de entrada .json '''
    ''' Arquivo de saida das informacoes '''
    ''' Nome da cidade a ser avaliada (MAYBE) '''
    parser.add_argument('-i','--input', help='input filename in json format', required=True)
    parser.add_argument('-o','--output', help='file to store the informations', required=True)
    parser.add_argument('-c','--city', help='City to be evaluate \n[Belo horizonte - Use "bh", Brasilia - "bsb", Curitiba - "ctba", Fortaleza - "forz", Manaus - "man",  Porto Alegre - "poal",  Recife - "rec", Rio de Janeiro - "rj", Sao Paulo - "sp", Salvador - "ssa"] ', required=True)
    parser.add_argument('-bd','--begindate', help='Begin date to be analyze')
    parser.add_argument('-ed','--enddate', help='End date to be analyze')
    args = vars(parser.parse_args())

''' Get number of tweets per selected users '''
def operations_tweet_total(database, output_information, city_name):

    arq = open (output_information+"_"+city_name+'.txt', 'a+')
    arq.write("#### Tweets per Users\n")
    arq.write("ID user\t # Tweets \n")
    
    # Print on Console the progress of the process
    # print "#### Tweets per Users"
    # Convert the arg city_name to a variable
    count_city = "count_"+city_name
    users_city = "user_names_"+city_name
    users_ids = "ids_"+city_name
    
    # loop to check the ids of selected user into each tweet
    # O(n^2)
    for user_id in eval(users_ids):
        temp = 0
        tweets_file = open(database, "r")
        for line in tweets_file:
            try:
                #Read in one line of the file, convert it into a json object 
                tweet = json.loads(line.strip())
                if 'text' in tweet:
                    # Tweet ID
                    user_id_comp = tweet['user']['id']
                    # Compare to static USER_ID
                    if(user_id_comp == user_id):
                        temp = temp + 1
                        #print "EEEE"
                        #print eval(count_city)
            except:
                # read in a line is not in JSON format (sometimes error occured)
                continue
        # test = "User_id\t" + str(user_id)  + "\t Qtd tweets \t" + str(temp) + "\n"
        result = str(user_id) +"\t"+ str(temp) +"\n"
        arq.write(result)
        # Print on Console the total of tweets of the user
        # print "User_id\t", user_id , "\t Qtd tweets \t", temp
        tweets_file.close()    
    arq.close()    
    

''' Count the mentions of the selected users '''
def operations_mentions(database, output_information, city_name):

    arq = open(output_information+"_"+city_name+'.txt', 'a+')
    arq.write("#### Mentions of the selected users \n")
    arq.write("ID user\t # Mentions \n")
    # Print on Console the progress of the process
    print "#### Mentions of the selected users"

    # Convert the arg city_name to a variable
    count_city = "count_"+city_name
    users_city = "user_names_"+city_name
    users_ids = "ids_"+city_name
   
    # loop to check the ids of selected user into each tweet
    for user_id in eval(users_ids):
        temp = 0
        tweets_file = open(database, "r")
        for line in tweets_file:
            try:
                #Read in one line of the file, convert it into a json object 
                tweet = json.loads(line.strip())
                if 'text' in tweet:
                    users_id_mention = [user_mention['id'] for user_mention in tweet['entities']['user_mentions']]
                    
                    for usersx_id in users_id_mention:
                        # Compare to user_id
                        if(usersx_id == user_id):
                            temp = temp + 1
            except:
                # read in a line is not in JSON format (sometimes error occured)
                continue
        # test = "User_id\t" + str(user_id)  + "\t Qtd tweets \t" + str(temp) + "\n"
        result = str(user_id) +"\t"+ str(temp) +"\n"
        # print test
        arq.write(result)
        # Print on Console the total of tweets mentions of the user
        # print "User_id\t", user_id , "\t Qtd tweets \t", temp
        tweets_file.close()    
    arq.close()    


''' Teste function'''
def operations_retweet(database, output_information, city_name):

    arq = open(output_information+"_"+city_name+'.txt', 'a+')
    arq.write("#### Retweet of the selected users \n")
    arq.write("ID user\t # Retweets\n")
    print "#### Retweet of the selected users"

    # Convert the arg city_name to a variable
    count_city = "count_"+city_name
    users_city = "user_names_"+city_name
    users_ids = "ids_"+city_name
    
    # loop to check the ids of selected user into each tweet
    for user_id in eval(users_ids):
        temp = 0
        tweets_file = open(database, "r")
        for line in tweets_file:
            try:
                #Read in one line of the file, convert it into a json object 
                tweet = json.loads(line.strip())

                if 'text' in tweet:
                    user_retweeted =  tweet['retweeted_status']['user']['id']
                    if(int(user_retweeted) == user_id):
                        temp = temp + 1
            except:
                # read in a line is not in JSON format (sometimes error occured)
                continue
        # test = "User_id\t" + str(user_id)  + "\t Qtd tweets \t" + str(temp) + "\n"
        result = str(user_id) +"\t"+ str(temp) +"\n"
        # print test
        arq.write(result)
        # Print on Console the total of tweets of the user
        # print "User_id\t", user_id , "\t Qtd tweets \t", temp
        tweets_file.close()    
    arq.close()    

'''MAIN FUNCTION'''
# Call the python with args
#python automatic_tweets_json_v1.py -i [1] -o [2] -c [3]

#[1] json file of the tweets with the seletected fields
#[2] name of the output file
#[3] name of the city to be evaluate

if ( __name__ == "__main__" ):
    get_parameters()
    # json file to be used to calculate the following informations: number of tweets, retweets and mentions.
    database = args['input']
    # file to store the information of tweets, mentions and retweets
    output_information = args['output']
    # name the city used to be verify
    city_name = args['city']
    # tweets of the users
    operations_tweet_total(database,output_information,city_name)
    # mentions of the users
    operations_mentions(database,output_information,city_name)
    # retweet of the users
	    operations_retweet(database,output_information,city_name)
