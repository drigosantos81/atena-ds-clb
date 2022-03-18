from sources.selecao_cortes_Copy11 import *

from functools import partial 
import ipywidgets as widgets

from ipywidgets import FileUpload

from IPython.display import display

from locale import currency as Moeda
from money import Money
from folium.plugins import MarkerCluster

from datetime import date
from hdbcli import dbapi

today = date.today()
#import dill
from copy import deepcopy
#import psutil

from pathos.pools import ProcessPool
#from p_tqdm import p_map
#from p_tqdm import p_umap

from io import BytesIO
#import time
from time import time
from time import strftime
from time import sleep

import threading

from psutil import virtual_memory
from psutil import cpu_percent
now = time()

from pathlib import Path
import arrow
import os
import re
#import locale




#locale.setlocale(locale.LC_MONETARY, 'pt_BR.UTF-8') 

class SelecaoLayout():
    utd = "EUNAPOLIS"

    # Filtrar por Zona. Para não filtra --> zona = None
    zona = " "
    # Filtrar por município: municipio = "RIACHO DE SANTANA"
    municipio = " "
    # Filtrar por bairro: bairro = "CENTRO"
    bairros = [""]
    #Filtrar por localidade: localide = 'SALVADOR'
    locali = [""]
    #Tipo da localização dos clientes 'R' para filtrar apenas do tipo rural, 'U' para urbano e None para sem filtro
    local = None
    # metodo de seleção
    metodo = 'nkcnk'
    metodo_sm = 'fast nkcnk' # sm: selecao massiva
    # Peso do mtvcob a ser considerado na seleção.
    peso_mtvcob = 1
    # Peso da pecld a ser considerado na seleção.
    peso_pecld = 1.3 
    #peso qtfvte
    peso_qtftve = 0
    # Definir se irar recalculor o Indice de Recuperação de Receita ou usar o da query sql.
    calcular_irr = False
    calcular_irr_sm = False
    #Define se quer utilizar o processamento em paralelo ou não
    multi_process_sm = True
    # Número de clusters
    n = 2
    # Número máximo de clientes em um clusters
    k = 25
    # Número mínimo de clientes que devem selecionados em um cluster
    min_selecionados = 22
    # Define uma maior precisão no raio máximo dos clusters, podendo diminuir o IRR selecionado
    r_max_preciso = False
    r_max_preciso_sm = True
    #importar dados conf massiva
    importar_conf_sm = True
    #Tabela Suscetiveis
    tabelasuscetiveis = 'SuscetiveisTabela'
    #opcoes turma
    opcoes_turma_sm = ['STC','EPS']
    #opcoes "grupos"
    opcoes_grupos_sm =  ['cluster_id','UTD','ZONA']
    grupo_sm = 'cluster_id'
    #selecionados_sm
    selecionados_sm = pd.DataFrame()
    #nselecionados_sm
    n_selecionados_sm = pd.DataFrame()
    #errossm
    erros_sm = pd.DataFrame()
    #pesos confg
    peso_mtv_cob_wgt_value = 1
    peso_pecld_wgt_value = 2
    peso_qtdftve_wgt_value = 0
    #plotagem massiva
    plotar_clusters_sm = False
    #opcoesconf
    opcoes_conf_sm = ['CONVENCIONAL','DISJUNTOR','RECORTE','BAIXA MIX','MISTA']
    opcoes_suscetiveis = ['CONVENCIONAL','DISJUNTOR','RECORTE','BAIXA MIX','MISTA']
    #Tabela Hana
    tabela_hana = 'CLB961920.ATENA_HIST'
    #carteira
    carteira_sm = 'CONVENCIONAL'
    r = 1000
    marc_iter = 0
    marc_insert = 0
    r_lista = [500, 1000, 1500, 2000]

    destaque_pecld_sm = 4000
    destaque_mtvcob_sm = 10000
    destaque_qtftve_sm = 1

    status_parametros = widgets.HTML("""""")
    html_resultado_testagem = widgets.HTML("""""")
    html_selecao = widgets.HTML(
        folium.Map(
            location=[-12.9869145, -43.8940578],
            zoom_start=7, prefer_canvas=True
        )._repr_html_()
    )

    html_selecao_massiva = widgets.HTML(
        folium.Map(
            location=[-12.9869145,-43.8940578],
            zoom_start=7, prefer_canvas=True
        )._repr_html_()
    )


    def __init__(self, usuario: str, senha: str):
        """
        """
        #Apaga os arquivos de resultados de seleções criados há mais de 5 dias
        #pensar em alternativas, pode ser um ponto de insegurança
        self.__usuario = usuario
        self.__senha = senha
        #self.excluir_arquivos_antigos(diretorio=r"d:\Users\ETR420480\Desktop\devel\recuperacao_energia\resultados", h=120)
        self.excluir_arquivos_antigos2('resultados/',5)
        #self.resultados_massiva = [f for f in os.listdir('resultados/') if (f.endswith('.xlsx' )and not(f.startswith('erros')))]
        self.container_suscetiveis()
        self.att_consulta_cluster_hist()
        #self.consulta(usuario,senha)
        
        
    def consulta_selecao(self,b,usuario,senha):
        #data pedido
        data_pedido = str(self.date_picker.value)
        #carteira ou sql a ser consultada
        carteira_sql = self.sql_select.value
        #Variável para definir se é a tabela de suscetiveis ou sqls 
        consulta = self.consulta_drop.value
        self.turma_sm = self.turma_drop.value
        try:
            self.status_consulta.value = "Consulta em Andamento"
            self.num_suscetiveis.value = 0

            if consulta == 'Tabela Suscetiveis':
                self.consulta_nome = self.turma_sm +"_"+self.sql_select.value
                self.nome_sql.value = self.consulta_nome
                self.carteira_sm = carteira_sql
                list_confg = ['sources/Sqls/'+self.tabelasuscetiveis+'.sql',carteira_sql,
                              data_pedido,self.turma_sm]
                self.selecao = SelecaoCorte(usuario, senha, consulta = list_confg)

            else:
                self.consulta_nome = carteira_sql[0:3]
                self.nome_sql.value = self.consulta_nome
                list_confg = ['sources/Sqls/'+carteira_sql]
                self.selecao = SelecaoCorte(usuario, senha, consulta= list_confg) 
            
            if 'nome_arquivo' in dir(self): 
                    self.nome_arquivo.value = self.consulta_nome+"_"+str("{:%d%m}".format(datetime.now()))
                    self.carteira_wgt.value =  self.carteira_sm 
                    if self.selecao.dfn.shape[0] > 0:
                        self.reset_configs_parametros()
           
            self.nome_selecao = self.consulta_nome+"_"+str("{:%d%m}".format(datetime.now()))
            self.selecao_m = deepcopy(self.selecao)
            #self.update_marc_insert()
            self.num_suscetiveis.value = len(self.selecao.dfn.index)
            self.status_consulta.value = "Consulta Concluída"
            self.importar_css()
           
        except Exception as e:
            print(e)
            
    def update_marcacao(self,arquivo):
        try:
            for i in arquivo.MARCACAO.unique():
                if arquivo.loc[arquivo['MARCACAO']==i]['MARC_ZONA'].unique()[:3][0] == 'MIX_':
                    if arquivo.loc[arquivo['MARCACAO'] == i].ZONA.unique().shape[0] == 1:
                        arquivo.loc[arquivo['MARCACAO']==i,'MARC_ZONA'] = arquivo.loc[arquivo['MARCACAO'] == i].ZONA.unique()[0]
                self.marc_insert = self.marc_insert + 1
                arquivo.loc[arquivo['MARCACAO']==i,'MARCACAO'] =arquivo.loc[arquivo['MARCACAO']==i]['MARC_ZONA'].astype(str) + str(self.marc_insert).zfill(4)
        except Exception as e:
            print(e)
        return arquivo
            
    def update_marc_insert(self):
        data = today
        sql = """SELECT MARCACAO FROM {tabela} 
                        WHERE DATA_PEDIDO = '{data}' 
                        GROUP BY MARCACAO """.format(tabela=self.tabela_hana,data = str(data))
       
        try:
            connection_hana = dbapi.connect(
                           address='BRNEO695',
                           port='30015',
                           user=self.__usuario,
                           password=self.__senha,
                           databasename='BNP',
                           sslValidateCertificate=False
            )
            cursor = connection_hana.cursor()
            marc_grav = pd.read_sql(sql, connection_hana)
            
            if marc_grav.shape[0]>0:
                marc_grav = marc_grav['MARCACAO'].str[-4:]
                marc_grav = marc_grav.astype(int)
                marc_grav_max = marc_grav.max()
                self.marc_insert = marc_grav_max
        except Exception as e:
            print(e)
        
    def reset_configs_parametros(self):
        
        self.utd = self.selecao.dfn['UTD'].sort_values().unique()[0] 
        self.zona = ' '
        self.locali = ['']
        self.bairros = ['']
        
        self.utd_text.options=self.selecao.dfn['UTD'].sort_values().unique().tolist()+ [""]
        self.utd_text.value = self.utd
        self.municipio_text.options = [' '] + \
                                         sorted(
                                             self.selecao.dfn[self.selecao.dfn.UTD == self.utd]
                                             ['ZCGMUNICI'].unique()
                                         )
        self.zona_text.options=[" "] + sorted(self.selecao.dfn[self.selecao.dfn.UTD == self.utd]['ZONA'].unique())

        self.municipio_text.value = self.municipio_text.options[0]                                     

        self.set_utd(self.utd_text)

        self.set_zona(self.zona_text)
 

        self.locali_text.value = ''
        self.bairros_text.value = ''
        self.status_parametros.value = """"""
        
    def consulta_drop_change(self,change):
        if change['type'] == 'change' and change['name'] == 'value':
            if change['new'] != change['old']:
                if change['new'] == 'Tabela Suscetiveis':
                    self.sql_select.options = self.opcoes_suscetiveis
                    self.date_picker.disabled = False
                else:
                    self.date_picker.value = today
                    self.date_picker.disabled = True
                    self.sql_select.options = self.sqls
                    
            self.sql_select.value = self.sql_select.options[0]
        
    def container_suscetiveis(self):
        #SQLS De suscetíveis
        conf_pixel = '0px 40px 10px 0px'
        self.sqls = [f for f in os.listdir('sources/Sqls') if f.endswith('.SQL')]
        #Layout e dropdown de Onde Realizar a Consulta
        label_consulta_drop = widgets.Label('Onde Consultar:',layout=widgets.Layout(width = '200px', height = '40px', 
                                                                                      margin = conf_pixel))
        self.consulta_drop = widgets.Dropdown(value ='Tabela Suscetiveis' ,options = ['Tabela Suscetiveis', 'Arquivo SQL'], 
                                         layout = widgets.Layout(width = '200px', height = '30px',margin =conf_pixel))
        #Container da Consulta
        container_consulta_drop  = widgets.HBox(children= [label_consulta_drop,self.consulta_drop])
        
        label_turma_drop = widgets.Label('Turma:', layout = widgets.Layout(width = '200px',height='30px',margin=conf_pixel))
        
        self.turma_drop = widgets.Dropdown(value =self.opcoes_turma_sm[0] ,options = self.opcoes_turma_sm,
                                           layout= widgets.Layout(width = '200px',height='30px',margin = conf_pixel))
        
        container_turma_drop = widgets.HBox(children = [label_turma_drop,self.turma_drop])
        #Observer para mudança da Consulta
        self.consulta_drop.observe(self.consulta_drop_change)
        #Layout e dropdown para Carteira ou SQL a ser consultada
        sql_select_label = widgets.Label('Selecione Carteira ou SQL:',layout=widgets.Layout(width = '200px', height = '40px', 
                                                                                  margin = conf_pixel))
        self.sql_select = widgets.Dropdown(options = self.opcoes_suscetiveis,layout=widgets.Layout(width = '200px', height = '30px', 
                                                                                  margin = conf_pixel))
        container_sql_select = widgets.HBox(children = [sql_select_label,self.sql_select])
        
        
        #Status da consulta, nome da consulta, etc...
        
        label_nome_sql = widgets.Label('Nome Consulta:',layout = widgets.Layout(width='200px', height='40px', margin=conf_pixel))
        self.nome_sql = widgets.Text(value = "", disabled = True,layout=widgets.Layout(width='200px', height='40px', margin=conf_pixel))
        
        label_numero_suscetiveis = widgets.Label('Quantidade de Suscetíveis:',layout=widgets.Layout(width='200px', height='40px', margin=conf_pixel))
        self.num_suscetiveis = widgets.IntText(disabled = True,value = 0, layout=widgets.Layout(width='200px', height='40px', margin=conf_pixel))
        
        label_status_consulta = widgets.Label('Status da Consulta:',layout=widgets.Layout(width='200px', height='40px',margin=conf_pixel))
        self.status_consulta = widgets.Text(value = "", disabled = True,  layout=widgets.Layout(width="200px", height='40px', margin=conf_pixel))
        
        label_date_picker = widgets.Label('Escolha a data de Pedido:', layout = widgets.Layout(width = '200px',height = '40px',
                                                                                              margin = conf_pixel))
        self.date_picker = widgets.DatePicker(value = today ,layout = widgets.Layout(width = '200px',height = '40px',
                                                                      margin = conf_pixel))
        
        container_nome = widgets.HBox(children = [label_nome_sql,self.nome_sql])
        container_numero = widgets.HBox(children = [label_numero_suscetiveis,self.num_suscetiveis])
        container_status = widgets.HBox(children = [label_status_consulta,self.status_consulta])
        container_data = widgets.HBox(children = [label_date_picker, self.date_picker])

        
        
    
        consulta_button = widgets.Button(description = "Realizar a Consulta",button_style = 'info', 
                                         layout = widgets.Layout(width = '440px',height = '40px'))
        consulta_button.on_click(partial(self.consulta_selecao,usuario=self.__usuario,senha=self.__senha))
            
                 
        container_geral = widgets.VBox(children = [container_consulta_drop,
                                                   container_turma_drop,
                                                     container_sql_select,
                                                     container_nome,
                                                     container_numero,
                                                     container_status,
                                                     container_data,
                                                     consulta_button])
        display(container_geral)


    def excluir_arquivos_antigos(self, diretorio, h):
        """
        Exclui os arquivos de um diretórios que foram criadas a mais de X horas
        :param diretorio: diretorio dos arquivos a serem esxluídos
        :param h: quantidade mínima de horas que um arquivo de ter para ser excluído
        """
        criticalTime = arrow.now().shift(hours=-h)
        for item in Path(diretorio).glob('*'):
            if item.is_file():
                itemTime = arrow.get(item.stat().st_mtime)
                if itemTime < criticalTime:
                    os.remove(item)
                    
    def excluir_arquivos_antigos2(self,path,dias):
        """
        Exclui os arquivos de um diretórios que foram criadas a mais de x dias
        :param path: diretorio dos arquivos a serem esxluídos
        :param dias: quantidade mínima de dias que um arquivo de ter para ser excluído
        """
        for filename in os.listdir(path):
            # if os.stat(os.path.join(path, filename)).st_mtime < now - 7 * 86400:
            if os.path.getmtime(os.path.join(path, filename)) < now - dias * 86400:
                if os.path.isfile(os.path.join(path, filename)):
                    #print(filename)
                    os.remove(os.path.join(path, filename))


    def importar_css(self):
        """
        Função que importa o estilo css como string
        """
        arquivo = open("static/style.css", "rt")
        self.style = arquivo.read()
        self.style = "<style>{}</style>".format(self.style)
        arquivo.close()              

    #  -------------- Inicio: Funções de setagem das variáveis de seleção -----------------
            
    def set_utd(self, wdgt):
        """
        Seta a UTD pela qual a seleção deve ser filtrada
        :param wdgt: instância widget
        """
        try:
            
            if wdgt.value == "" or wdgt.value == " ":
                self.utd = None
            elif wdgt.value not in self.selecao.dfn.UTD.unique():
                self.status_parametros.value = """<label class="status-label error-label">Nenhuma UTD encontrada com esse nome.</label>"""
            else:
                utd = wdgt.value.upper().replace("_", " ")
                self.utd = utd

        except NameError:
            self.status_parametros.value = """<label class="status-label error-label">UTD inválida</label>"""


    def utd_text_change(self, change):
        """
        De Acordo com a UTD selecionada, altera as opções de zona e muncípio
        :param change: dicionário com evento do widget
        """
        if change['type'] == 'change' and change['name'] == 'value':
            if change['new'] == "":
                self.zona_text.options = [' '] + \
                                         sorted(
                                             self.selecao.dfn
                                             ['ZONA'].unique()
                                         )
                self.municipio_text.options=[" "] + list(self.selecao.dfn['ZCGMUNICI'].sort_values().unique())
            else: 
                self.zona_text.options = [' '] + \
                                         sorted(
                                             self.selecao.dfn[self.selecao.dfn.UTD == change['new']]
                                             ['ZONA'].unique()
                                         )
                self.zona_text.value = self.zona_text.options[0]
                self.municipio_text.options = [' '] + \
                                         sorted(
                                             self.selecao.dfn[self.selecao.dfn.UTD == change['new']]
                                             ['ZCGMUNICI'].unique()
                                         )
            self.municipio_text.value = self.municipio_text.options[0]


    def set_zona(self, wdgt):
        """
        Seta a UTD pela qual a seleção deve ser filtrada
        :param wdgt: instância widget
        """
        try:
            zona = wdgt.value
            if zona == ' ':
                self.zona = None
            elif zona not in self.selecao.dfn.ZONA.unique():
                self.status_parametros.value = """<label class="status-label error-label">Nenhuma zona encontrada com esse nome.</label>"""
            else:
                self.zona = list(zona.split(','))
        except NameError:
            self.status_parametros.value = """<label class="status-label error-label">Zona inválida</label>"""


    def zona_text_change(self, change):
        """
        De Acordo com a zona selecionada, altera as opções de zona e muncípio
        :param change: dicionário com evento do widget
        """
        if change['type'] == 'change' and change['name'] == 'value':
            if change['new'] != change['old']:
                if change['new'] != ' ':
                    self.municipio_text.options = [' '] + \
                                             sorted(
                                                 self.selecao.dfn[self.selecao.dfn.ZONA == change['new']]
                                                 ['ZCGMUNICI'].unique()
                                             )
                else:
                    self.municipio_text.options = [' '] + sorted(self.selecao.dfn['ZCGMUNICI'].unique())
                self.municipio_text.value = self.municipio_text.options[0]


    def set_municipio(self, wdgt):
        """
        Seta o município pelo qual a seleção deve ser filtrada
        :param wdgt: instância widget
        """
        try:
            municipio = wdgt.value.upper().replace("_", " ")
            if municipio.replace(" ", "") == "":
                self.municipio = None
            elif municipio not in self.selecao.dfn.ZCGMUNICI.unique():
                self.status_parametros.value = """<label class="status-label error-label">Nenhum município encontrado com esse nome.</label>"""
            else:
                self.municipio = list(municipio.split(','))
        except NameError:
            self.status_parametros.value = """<label class="status-label error-label">Município inválido</label>"""


    def set_bairros(self, wdgt):
        """
        Seta os bairros pelos quais a seleção deve ser filtrada
        :param wdgt: instância widget
        """
        try:
            #ToDo refatorar
            bairros = wdgt.value.upper().replace(";", ",").strip()
            bairros = re.sub(r'\s+([,;"])', r'\1', bairros)
            bairros = re.sub(r'([,;"])+\s', r'\1', bairros)
            bairros = list(bairros.split(','))
            if bairros == [""]:
                self.bairros = None
            elif self.selecao.dfn[self.selecao.dfn.ZCGBAIRRO.isin(bairros)]['ZCGBAIRRO'].count() == 0: #ToDo corrigir
                self.status_parametros.value = """<label class="status-label error-label">
                        Não existem clientes suscetíveis nos bairros indicados. Confira se escreveu corretamente
                    </label>"""
            else:
                self.bairros = bairros
        except NameError:
            self.status_parametros.value = """<label class="status-label error-label">Bairro inválido</label>"""

    def set_locali(self,wdgt):
        """
        Seta as localidades pelos quais a seleção deve ser filtrada
        :param wdgt: instância widget
        """
        try:
            #ToDo refatorar
            locali = wdgt.value.upper().replace(";", ",").strip()
            locali = re.sub(r'\s+([,;"])', r'\1', locali)
            locali = re.sub(r'([,;"])+\s', r'\1', locali)
            locali = list(locali.split(','))
            if locali == [""]:
                self.locali = None
            elif self.selecao.dfn[self.selecao.dfn.ZCGLOCALI.isin(locali)]['ZCGLOCALI'].count() == 0: #ToDo corrigir
                self.status_parametros.value = """<label class="status-label error-label">
                        Não existem clientes suscetíveis nas localidades indicadas. Confira se escreveu corretamente
                    </label>"""
            else:
                self.locali = locali 
        except:
            self.status_parametros.value = """<label class="status-label error-label">Localidade inválida</label>"""
        
        
        
    def set_local(self, wdgt):
        """
        Seta o tipo de local pelo qual a seleção deve ser filtrada; Ex: 'U' (Urbano) e 'R' (Rural)
        :param wdgt: instância widget
        """
        try:
            self.local = wdgt.value
        except:
            print("Método inválido.")


    def set_metodo(self, wdgt):
        try:
            self.metodo = wdgt.value
        except:
            print("Método inválido.")
    
    def set_metodo_sm(self, wdgt):
        try:
            self.metodo_sm = wdgt.value
        except:
            print("Método inválido.")


    def set_peso_pecld(self, wdgt):
        """
        Seta o peso da variável PECLD_CONS na seleção dos cortes
        :param wdgt: instância widget
        """
        try:
            self.peso_pecld = float(wdgt.value.replace(",", "."))
        except:
            self.status_parametros.value = """<label class="status-label error-label">Número do peso da pecld inválido. Digite um número inteiro ou decimal.</label>"""


    def set_peso_mtvcob(self, wdgt):
        """
        Seta o peso da variável MTVCOB (montante de cobrança) na seleção dos cortes
        :param wdgt: instância widget
        """
        try:
            self.peso_mtvcob = float(wdgt.value.replace(",", "."))
        except:
            self.status_parametros.value = """<label class="status-label error-label">Número do peso do mtvcob inválido. Digite um número inteiro ou decimal.</label>"""
            
            
    def set_peso_qtftve(self, wdgt):
        """
        Seta o peso da variável MTVCOB (montante de cobrança) na seleção dos cortes
        :param wdgt: instância widget
        """
        try:
            self.peso_qtftve = float(wdgt.value.replace(",", "."))
        except:
            self.status_parametros.value = """<label class="status-label error-label">Número do peso da qtftve inválido. Digite um número inteiro ou decimal.</label>"""         
            
    
    def set_calcular_irr(self, wdgt):
        """
        Define se o IRR será calculado ou se será usado o IRR da consulta sql
        :param wdgt: instância widget
        """
        try:
            self.calcular_irr = bool(wdgt.value) 
        except:
            self.status_parametros.value = """<label class="status-label error-label">Calcular irr: Valor inválido.</label>"""
    

    def set_calcular_irr_sm(self, wdgt):
        """
        Define se o IRR será calculado ou se será usado o IRR da consulta sql
        :param wdgt: instância widget
        """
        try:
            self.calcular_irr_sm = bool(wdgt.value) 
        except:
            pass

    def set_multiprocess_sm(self,wdgt):
        """
        Define se será utilizado o processamento em paralelo, ou não
        :param wdgt: instância widget
        """
        try:
            self.multi_process_sm = bool(wdgt.value)
        except:
            pass
        
    def set_n(self, wdgt):
        """
        Seta a quantida de clusters a serem selecionados
        :param wdgt: instância widget
        """
        try:
            self.n = int(wdgt.value)
        except:
            self.status_parametros.value = """<label class="status-label error-label">Número de clusters inválido. Digite um número inteiro.</label>"""


    def set_k(self, wdgt):
        """
        Seta a quantidade de clientes máxima por cluster
        :param wdgt: instância widget
        """
        try:
            self.k = int(wdgt.value)
        except:
            self.status_parametros.value = """<label class="status-label error-label">Número máximo de clientes em um cluster inválido. Digite um número inteiro.</label>"""


    def set_min_selecionados(self, wdgt):
        """
        Seta a quantidade de clientes mínima DESEJADA por cluster
        :param wdgt: instância widget
        """
        try:
            self.min_selecionados = int(wdgt.value)
        except:
            self.status_parametros.value = """<label class="status-label error-label">Número mínimo desejado de clientes em um cluster inválido. Digite um número inteiro.</label>"""


    def set_r_max_preciso(self, wdgt):
        """
        Define a maior precisão no raio máximo dos clusters selecionados
        :param wdgt: instância widget
        """
        try:
            self.r_max_preciso = bool(wdgt.value) 
        except:
            self.status_parametros.value = """<label class="status-label error-label">Precisão do raio máximo. Valor inválido.</label>"""

    def set_r_max_preciso_sm(self, wdgt):
        """
        Define a maior precisão no raio máximo dos clusters selecionados
        :param wdgt: instância widget
        """
        try:
            self.r_max_preciso_sm = bool(wdgt.value) 
        except:
            self.status_parametros_sm.value = """<label class="status-label error-label">Precisão do raio máximo. Valor inválido.</label>"""
    def set_importar_conf_sm(self,wdgt):
        """
        Define se a importação da conf deve vir do upload ou da variável na memória
        """
        try:
            self.importar_conf_sm = bool(wdgt.value)
        except:
            self.status_parametros_sm.value = """<label class="status-label error-label">Importação da Configuração. Valor inválido.</label>"""
            
    def set_plotar_clusters_sm(self,wdgt):
        """
        Define se os clusters devem ser plotados no mapa ou não
        """
        try:
            self.plotar_clusters= bool(wdgt.value)
        except:
            self.status_parametros_sm.value = """<label class="status-label error-label">Plotar Clusters. Valor inválido.</label>"""          

    def set_r(self, wdgt):
        """
        Seta o raio de clusterização desejado
        :param wdgt: instância widget
        """
        try:
            self.r = int(wdgt.value)
        except:
            print("Raio de seleção inválido. Digite um número inteiro.")
    
    def set_r(self, wdgt):
        try:
            self.r = int(wdgt.value)
        except:
            print("Raio de seleção inválido. Digite um número inteiro.")
    
    def set_destaque_pecld_sm(self, wdgt):
        try:
            self.destaque_pecld_sm = int(wdgt.value)
        except:
            print("PECLD de destaque inválida. Digite um número inteiro.")

    def set_destaque_mtvcob_sm(self, wdgt):
        try:
            self.destaque_mtvcob_sm = int(wdgt.value)
        except:
            print("MTVCOB de destaque inválido. Digite um número inteiro.")
    
    def set_destaque_qtftve_sm(self, wdgt):
        try:
            self.destaque_qtftve_sm = int(wdgt.value)
        except:
            print("QTFTVE de destaque inválido. Digite um número inteiro.")

    def set_r_lista(self, wdgt):
        """
        Seta a lista de raios de clusterização para os quais serão realizadas seleções de teste.
        :param wdgt: instância widget
        """
        try:
            self.r_lista = list(map(int, wdgt.value.split(',')))
        except:
            print("Lista em um formato inválido")

    def set_carteira_sm(self,wdgt):
        try:
            self.carteira_sm = wdgt.value
        except Exception as e:
            print("Ocorreu um erro: ",e)
            
    def set_grupo_inserir_sm(self,wdgt):
        try:
            self.grupo_sm = wdgt.value
        except Exception as e:
            print("Ocorreu um erro: ",e)

            
    def save_sm_excel(self):
        try:
            self.selecionados_sm.to_excel('resultados/'+self.nome_selecao+'.xlsx', index=False)
        except Exception as e:
            print(e)            
    #  -------------- Fim: Funções de setagem das variáveis de seleção -----------------

    def set_contas_list(self,b):
        
        try:
            lista = list(map(str, b.split(',')))
            return lista
        except:
            print("Lista em um formato inválido")
            
    def set_cluster_id(self):
        clusterid = ""
        if self.utd != " " and self.utd != None:
            clusterid = str(self.utd) 
        if self.zona != " " and self.zona != None:
            clusterid = str(self.zona)
        if self.municipio != " " and self.municipio != None:
            clusterid = clusterid + ("_".join(self.municipio))
        if self.bairros != [""] and self.bairros != " " and self.bairros != None:
                clusterid = clusterid +("_".join(self.bairros))
        if self.locali !=[""] and self.locali !=" " and self.locali != None:
                clusterid = clusterid + ("_".join(self.locali))
        return clusterid
    #  -------------- Inicio: Funções auxiliares das tabs da interface -----------------
    def salvar_parametros(self, b):
        """
        Realiza a setagem dos parâmetros de seleção após o clique no botão "Salvar" da tab de parâmetros
        :param b: instância widget
        """
        self.status_parametros.value = ''
        self.set_utd(self.utd_text)
        self.set_zona(self.zona_text)
        self.set_locali(self.locali_text)
        self.set_municipio(self.municipio_text)
        self.set_bairros(self.bairros_text)
        self.set_peso_pecld(self.peso_pecld_text)
        self.set_peso_mtvcob(self.peso_mtvcob_text)
        self.set_peso_qtftve(self.peso_qtftve_text)
        self.set_calcular_irr(self.calcular_irr_text)
        self.set_metodo(self.metodo_text)
        self.set_local(self.local_text)
        self.set_n(self.n_text)
        self.set_k(self.k_text)
        self.set_min_selecionados(self.min_selecionados_text)
        self.set_r_max_preciso(self.r_max_preciso_text)

        if not self.status_parametros.value:
            self.status_parametros.value = """<label class="status-label success-label">Os parâmetros da seleção foram atualizados!</label>"""
    
    
    def resultados_sm(self):
      
        if self.selecionados_sm.shape[0] > 0:
            clientes = self.selecionados_sm.shape[0]
            mtv = Money(self.selecionados_sm.MTV_COB.sum(),'BRL').format('es_ES')
            pecld = Money(self.selecionados_sm.PECLD.sum(),'BRL').format('es_ES')
            zonas =  self.selecionados_sm['ZONA'].nunique()
        else:
            clientes = 0
            mtv = Money(0,'BRL').format('es_ES')
            pecld = Money(0,'BRL').format('es_ES')
            zonas = 0
            
        n_selecionadas = self.erros_sm.shape[0]
        
        return clientes,mtv,pecld,zonas,n_selecionadas
    def tabela_resultado_sm(self):
        """
        :return um html com o resultado da seleção massiva
        """
        clientes,mtv,pecld,zonas,n_selecionadas = self.resultados_sm()
        tabela_resultados = """
                              <table>
                               <tr>
                                    <th>CONTAS SELECIONADAS</th>
                                    <th>MTV</th>
                                    <th>PECLD</th>
                                    <th>ZONAS</th>
                                    <th>ERROS SELEÇÃO</th>
                                <tr>
                                    <th>{0} Contas
                                    <th>{1} 
                                    <th>{2}
                                    <th>{3}
                                    <th>{4}
                                </tr>
                              """
        
        tabela_resultados = tabela_resultados.format(clientes,mtv,pecld,zonas,n_selecionadas)
        self.html_tabela_resultados_sm.value = tabela_resultados
        
        
        
        

    def tabela_resultado_selecao(self):
        """
        :return: retorna o html com o resultado geral da seleção.
        """
        tabela_resultados = """<table>
                            <tr>
                                <th>CLUSTER</th>
                                <th>CLIENTES</th>
                                <th>RAIO (m)</th>
                                <th>MTVCOB</th>
                                <th>PECLD CONS</th>
                          """

        resultado = self.selecao.resultados()
        mtv_metro_tot = 0
        pecld_metro_tot = 0
        r_tot = 0

        for cluster in resultado:
            if cluster[0] == 'TOTAL':
                if r_tot ==0:
                    r_tot =1
                tabela_resultados += """
                                            <tr>
                                                <td>{0}</td>
                                                <td>{1} clientes</td>
                                                <td>{2}</td>
                                                <td><strong>{3} ({4}) </strong></td>
                                                <td><strong>{5} ({6}) </strong></td>
                                            </tr>
                                         """.format(cluster[0], cluster[1], cluster[2],
                                                    Money(cluster[3], 'BRA').format('es_ES'),
                                                    Money(mtv_metro_tot / r_tot, 'BRA').format('es_ES'),
                                                    Money(cluster[4], 'BRA').format('es_ES'),
                                                    Money(pecld_metro_tot / r_tot, 'BRA').format('es_ES')
                                                    )
            else:
                if cluster[2] == 0:
                    cluster[2] =1
                mtv_metro = round(cluster[3] / cluster[2], 2)
                pecld_metro = round(cluster[4] / cluster[2], 2)

                r_tot += cluster[2]
                if r_tot ==0:
                    r_tot = 1
                pecld_metro_tot += cluster[4]
                mtv_metro_tot += cluster[3]
                

                tabela_resultados += """
                                            <tr>
                                                <td>{0}</td>
                                                <td>{1} clientes</td>
                                                <td>{2} metros </td>
                                                <td>{3} ({4})</td>
                                                <td>{5} ({6})</td>
                                            </tr>
                                         """.format(cluster[0],
                                                    cluster[1],
                                                    cluster[2],
                                                    Money(cluster[3], 'BRA').format('es_ES'),
                                                    Money(mtv_metro, 'BRA').format('es_ES'),
                                                    Money(cluster[4], 'BRA').format('es_ES'),
                                                    Money(pecld_metro, 'BRA').format('es_ES')
                                                    )

        tabela_resultados += "</table>"

        return tabela_resultados
    
    def att_consulta_cluster_hist(self):
        """
         Atualiza os resultados da seleção
        :param b: instância widget
        """        
        self.resultados_massiva = [f for f in os.listdir('resultados/') if (f.endswith('.xlsx' )and not(f.startswith('erros')))]
        try:
            self.arquivo_hana.options = self.resultados_massiva
            self.dropdown_selecao.options = self.resultados_massiva
        except:
            pass
    def utilization_percent(self):
        while True:
            self.percent_cpu.value = str(cpu_percent())+'%'
            self.percent_ram.value = str(virtual_memory()[2])+'%'
            sleep(2)
            
        
        
    def importar_clusterconf(self, b):
        self.status_consulta_hana.value = """<label class="status-label warning-label">Importando Configuração...</label>"""
        self.botao_importar_conf.disabled = True
        self.set_carteira_sm(self.carteira_wgt)
        self.arquivo_conf_zonas = ""
        turma = self.turma_sm
        carteira = turma + "_"+self.carteira_sm
        if turma == 'STC':
            carteira_import = 'PROPRIA'
        else:
            carteira_import = self.carteira_sm
            
            sql_conf = """SELECT  ZCGUTD AS UTD, ZCGIDZONA AS ZONA, QTD_MIN AS NUM_CORTES_MIN, QTD_MAX AS NUM_CORTES_IDEAL, NUM_CLUSTER AS  CLUSTERS, CAST(RAIO_MIN*1000 AS int) AS RAIO_IDEAL, 
        CAST(RAIO_MAX*1000 AS int) AS RAIO_MAX, CAST(RAIO_INC*1000 AS int) AS RAIO_STEP,CARTEIRA, ZCGQTFTVE_MIN AS MIN_QTDFTVE 
        FROM CLB142840.CLUSTER_CONFIG4
        WHERE CARTEIRA = '{cart}' AND SUBSTRING(DIASEM_SOL,
            CASE 
                WHEN WEEKDAY('{data}') = 0 THEN 1 -- SEGUNDA
                WHEN WEEKDAY('{data}') = 1 THEN 2 -- TERÇA
                WHEN WEEKDAY('{data}') = 2 THEN 3 -- QUARTA
                WHEN WEEKDAY('{data}') = 3 THEN 4 -- QUINTA
                WHEN WEEKDAY('{data}') = 4 THEN 5 -- SEXTA
                WHEN WEEKDAY('{data}') = 5 THEN 6 -- SABADO
                WHEN WEEKDAY('{data}') = 6 THEN 7-- DOMINGO
            END ,1) = 1""".format(cart=carteira_import, data = str(self.date_picker.value))

        try:
            connection_hana = dbapi.connect(
                           address='BRNEO695',
                           port='30015',
                           user=self.__usuario,
                           password=self.__senha,
                           databasename='BNP',
                           sslValidateCertificate=False
            )
            cursor = connection_hana.cursor()
            conf = pd.read_sql(sql_conf, connection_hana)
            conf['CARTEIRA'] = self.carteira_sm
            conf['TURMA']= turma
            conf['PESO_MTVCOB'] = self.peso_mtv_cob_wgt.value
            conf['PESO_PECLD'] = self.peso_pecld_wgt.value
            conf['PESO_QTDFTVE']= self.peso_qtdftve_wgt.value
            conf['SELECIONAR'] = 'SIM'
            conf['TIPO_SELECAO'] = ""
            conf['MUNICIPIO'] = ""
            conf['LOCALI'] = ""
            conf['SERVICO'] = ""
            conf['BAIRRO'] = ""
            conf['TIPO_LOCAL'] = ""
            conf = conf[['UTD','SELECIONAR','ZONA','LOCALI','MUNICIPIO','BAIRRO','TIPO_LOCAL','CLUSTERS','NUM_CORTES_IDEAL','NUM_CORTES_MIN','RAIO_IDEAL','RAIO_MAX','RAIO_STEP','CARTEIRA','TURMA','PESO_MTVCOB','PESO_PECLD','PESO_QTDFTVE','MIN_QTDFTVE','SERVICO']]
            #'MIN_QTDFTVE'

            conf = conf[conf['CLUSTERS'] != 0]
            conf = conf[conf['NUM_CORTES_MIN'] != 0]
            self.conf_sm = conf
            conf.to_excel('Configuracoes/'+'CONFIGURACAO_'+str(carteira)+'.xlsx',index=False)
            conf.to_csv('Configuracoes/' + 'CONFIGURACAO_'+str(carteira)+'.csv',index=False,sep=';',decimal=',')
            
            self.status_consulta_hana.value = """<label class="status-label success-label">Foram importadas {linhas} linhas de configuração.</label>""".format(linhas = conf.shape[0])
            self.resultado_conf.value = """<a href='Configuracoes/{val}.xlsx' target='_blank'>Arquivo de Configuração xlsx</a><br>
        """.format(val='CONFIGURACAO_'+str(carteira))
            self.resultado_conf_csv.value = """<a href='Configuracoes/{val}.csv' target='_blank'>Arquivo de Configuração csv</a><br>
        """.format(val='CONFIGURACAO_'+str(carteira))
            
            self.botao_importar_conf.disabled = False
            connection_hana.close()  
        except Exception as e:
            self.botao_importar_conf.disabled = False
            self.status_consulta_hana.value = """<label class="status-label error-label">Ocorreu um Erro, {e}</label>""".format(e = e)
     
            
            

      
    def cluster_hist(self,b):
        """
        Insere os resultados selecionados na Tabela definida ple variável self.tabela_hana
        :param b: instância widget
        """        
        
        self.botao_inserir_cluster.disabled = True
        self.status_consulta_hana.value = """<label class="status-label warning-label">Inserindo seleção na Tabela do Hana...</label>"""
        try: 
            flag_defasagem = self.flag_def.value
            self.set_carteira_sm(self.carteira_wgt)
            data = today
            turma = self.turma_sm

            #Lê o Arquivo de Resultados e Modifica o Nome das Colunas para dar Match na Table do Hana

            arquivo_hist = pd.read_excel('resultados/' + self.arquivo_hana.value,converters={'ZCGACCOUN': str,'ZONA': str, 'MARCACAO': str,'MARC_ZONA': str})
            self.update_marc_insert()

            arquivo_hist['DATA_PEDIDO'] = data
            arquivo_hist['FLAG_DEF'] = flag_defasagem   
          

            arquivo_hist['MTV_COB'] = arquivo_hist['MTV_COB'].fillna(0.0)
            arquivo_hist['PECLD'] = arquivo_hist['PECLD'].fillna(0.0)
            
            arquivo_hist['UTD'] = arquivo_hist['UTD'].fillna('None')
            arquivo_hist['ZCGUTD'] = arquivo_hist['UTD']

            arquivo_hist['LATITUDE'].replace('None', 0.0, inplace=True)
            arquivo_hist['LONGITUDE'].replace('None', 0.0, inplace=True)
            arquivo_hist['LATITUDE'].replace('none', 0.0, inplace=True)
            arquivo_hist['LONGITUDE'].replace('none', 0.0, inplace=True)

            arquivo_hist['LATITUDE'] = arquivo_hist['LATITUDE'].fillna(0.0)
            arquivo_hist['LONGITUDE'] = arquivo_hist['LONGITUDE'].fillna(0.0)
            


            arquivo_hist['UTD'] = arquivo_hist['ZCGUTD']+ "_" + arquivo_hist['ZONA'].astype(str)
            arquivo_hist['ZCGIDZONA'] = arquivo_hist['ZONA'].loc[arquivo_hist['ZONA'] != 0].str.lstrip('0')

            arquivo_hist['ZCGIDZONA'] =   arquivo_hist['ZCGIDZONA'].fillna(0)
            arquivo_hist['ZCGIDZONA'].replace('none',0, inplace=True)
            arquivo_hist['ZCGIDZONA'].replace('None',0, inplace=True)
            arquivo_hist['ZCGIDZONA'].replace('NONE',0, inplace=True)

            arquivo_hist['USUARIO'] = str(self.__usuario).upper()
            arquivo_hist['HORA'] = strftime("%H:%M:%S")
            if 'cluster' in arquivo_hist.columns:
                arquivo_hist = self.update_marcacao(arquivo_hist)
                arquivo_hist['cluster'] = arquivo_hist['MARCACAO'].str[-4:].str.lstrip('0')
                #arquivo_hist['MARCACAO'] =   arquivo_hist['cluster'].astype("string").str.zfill(4)
                if 'CARTEIRA' in arquivo_hist.columns:
                    arquivo_hist['CARTEIRA'] = arquivo_hist['CARTEIRA'].fillna(arquivo_hist['CARTEIRA'].value_counts().idxmax())
                    arquivo_hist['TURMA'] = arquivo_hist['TURMA'].fillna(arquivo_hist['TURMA'].value_counts().idxmax())
                    arquivo_hist = arquivo_hist.loc[:,['UTD','MARCACAO','ZCGACCOUN','ZCGUTD','ZCGIDZONA','DATA_PEDIDO','FLAG_DEF','MTV_COB','PECLD','LATITUDE','LONGITUDE', 'RAIO','cluster','CARTEIRA','USUARIO','HORA','TURMA']] 
                else:
                    arquivo_hist['CARTEIRA'] = self.carteira_sm 
                    arquivo_hist['TURMA'] = turma
                    arquivo_hist = arquivo_hist.loc[:,['UTD','MARCACAO','ZCGACCOUN','ZCGUTD','ZCGIDZONA','DATA_PEDIDO','FLAG_DEF','MTV_COB','PECLD','LATITUDE','LONGITUDE', 'RAIO','cluster','CARTEIRA','USUARIO','HORA','TURMA']] 
            else:
                arquivo_hist['CARTEIRA'] = self.carteira_sm 
                arquivo_hist['TURMA'] = turma
                arquivo_hist = arquivo_hist.loc[:,['UTD','ZCGACCOUN','ZCGUTD','ZCGIDZONA','DATA_PEDIDO','FLAG_DEF','MTV_COB','PECLD','LATITUDE','LONGITUDE','CARTEIRA','USUARIO','HORA','TURMA']] 


            arquivo_hist = arquivo_hist.rename(columns = {"MTV_COB":"ZCGMTVCOB", "PECLD":"ZCGPECLD"})


            try:
                connection_hana = dbapi.connect(
                               address='BRNEO695',
                               port='30015',
                               user=self.__usuario,
                               password=self.__senha,
                               databasename='BNP',
                               sslValidateCertificate=False
                )

                cursor = connection_hana.cursor()

                sql_consulta = """SELECT ZCGACCOUN, DATA_PEDIDO,FLAG_DEF, USUARIO FROM {tabela} WHERE ("DATA_PEDIDO" = {data})""".format(tabela = self.tabela_hana,data="'"+str(data)+"'")

                gravados = pd.read_sql(sql_consulta, connection_hana)
                #self.teste = gravados
                if gravados.shape[0] > 0:
                    arquivo_hist['COMPARACAO'] = (arquivo_hist.ZCGACCOUN.isin(gravados.ZCGACCOUN) & arquivo_hist.DATA_PEDIDO.isin(gravados.DATA_PEDIDO))

                    arquivo_hist = arquivo_hist[arquivo_hist['COMPARACAO'] == False]
                    arquivo_hist = arquivo_hist.drop(columns=['COMPARACAO']) 


                columns = ','.join(arquivo_hist.columns)
                columns = [columns.replace('cluster', '"cluster"')]
                columns = ','.join([str(data) for data in columns])


                values=','.join([':{:d}'.format(i+1) for i in range(len(arquivo_hist.columns))])
                sql = 'INSERT INTO {tabela}({columns:}) VALUES ({values:})'

                if arquivo_hist.shape[0] > 0:
                    cursor.executemany(sql.format(tabela = self.tabela_hana,columns=columns, values=values), arquivo_hist.values.tolist())
                connection_hana.close()
                self.status_consulta_hana.value = """<label class="status-label success-label">Foram inseridas {val} linhas </label>""".format(val=len(arquivo_hist.index))
                #connection_hana.close()    

            except Exception as e:
                self.status_consulta_hana.value = """<label class="status-label error-label">Ocorreu um Erro: {e}</label>""".format(e = e)
        except Exception as e:
            self.status_consulta_hana.value = """<label class="status-label error-label">Ocorreu um Erro: {e}</label>""".format(e = e)
 
        
        self.botao_inserir_cluster.disabled = False
        

    def realizar_selecao(self, b):
        """
        Realiza a seleção a partir do raio indicado e atualiza o html da tab de seleção.
        :param b: instância widget
        """
        #Desabilita o botão "Realizar seleção"
        self.botao_realizar_selecao.disabled = True
        self.status_selecao.value = """<label class="status-label warning-label">Seleção em andamento...</label>"""

        self.set_r(self.r_text)
        self.html_selecao.value = ''
        self.status_parametros.value = ''
        try:
            mapa = self.selecao.selecionar(metodo=self.metodo, peso_mtvcob=self.peso_mtvcob,
                                           peso_pecld=self.peso_pecld,
                                           peso_qtftve = self.peso_qtftve,
                                           calcular_irr=self.calcular_irr,
                                           UTD=self.utd, zona=self.zona, locali=self.locali,
                                           municipio=self.municipio,
                                           bairros=self.bairros,
                                           n=self.n, k=self.k, r=self.r,
                                           min_selecionados=self.min_selecionados, 
                                           r_max_preciso=self.r_max_preciso,
                                           local=self.local, plot=True)
            
            self.html_selecao.value = self.tabela_resultado_selecao()
            
            self.html_selecao.value += mapa._repr_html_()

            self.html_selecionados.value = self.selecao.selecionados.sort_values('MTV_COB', ascending=False)\
                                                                      [['ZCGACCOUN', 'UTD', 'ZONA',
                                                                      'ZCGMUNICI', 'ZCGBAIRRO',
                                                                      'MTV_COB', 'PECLD',
                                                                      'cluster']].to_html()
     
            self.add_cliente_cluster_text.options = [self.selecao.nomes_clusters[i] for i in range(self.n)]
            self.add_cliente_cluster_text.value = self.add_cliente_cluster_text.options[0]
            self.set_cliente_cluster_cluster_text.options = [self.selecao.nomes_clusters[i] for i in range(self.n)]
            
            self.set_cliente_cluster_cluster_text.value = self.set_cliente_cluster_cluster_text.options[0]
            
            for i in self.selecao.selecionados.MARCACAO.unique():
                self.marc_iter = self.marc_iter + 1
                self.selecao.selecionados.loc[self.selecao.selecionados['MARCACAO'] ==i,'MARCACAO'] = self.selecao.selecionados.loc[self.selecao.selecionados['MARCACAO']==i]['MARC_ZONA'] + str(self.marc_iter).zfill(4)
            self.status_selecao.value = ''
            #self.selecao.selecionados = self.selecao.selecionados.drop(columns=['MARC_ZONA'])
        except Exception as e:
            
            self.status_selecao.value = """<label class="status-label error-label">Erro na Seleção. Possível Falta de Suscetíveis , {e}</label>""".format(e = e)
        self.botao_realizar_selecao.disabled = False

    def selecao_add_cliente(self, b):
        """
        Adciona um cliente susceptível ao corte na seleção
        :param b: instância widget
        """
        # Todo Add um try
        cc = self.add_cliente_cc_text.value
        cluster = self.add_cliente_cluster_text.value
        mapa = self.selecao.selecionar_clientes(contas=[(cc, cluster)], plot=True)
        self.html_selecao.value = self.tabela_resultado_selecao()
        self.html_selecao.value += mapa._repr_html_()
        self.add_cliente_cc_text.value = ""
        self.html_selecionados.value = self.selecao.selecionados[['ZCGACCOUN', 'UTD', 'ZONA',
                                                                  'ZCGMUNICI', 'ZCGBAIRRO',
                                                                  'MTV_COB', 'PECLD',
                                                                  'cluster']].to_html()


    def selecao_set_cliente_cluster(self, b):
        """
        Seta o cluster de um cliente já selecionado. Usada na prática para trocar mudar um cliente selecionado de cluster.
        :param b: instância widget
        """
        # Todo Add um try
        try:
            cc = self.set_cliente_cluster_cc_text.value
            cluster = self.set_cliente_cluster_cluster_text.value.replace(" ", "")
            mapa = self.selecao.set_cluster(contas=[(cc, cluster)], plot=True)
            self.html_selecao.value = self.tabela_resultado_selecao()
            self.html_selecao.value += mapa._repr_html_()
            self.set_cliente_cluster_cc_text.value = ""
            #self.set_cliente_cluster_cluster_text.value = ""

            self.html_selecionados.value = self.selecao.selecionados[['ZCGACCOUN', 'UTD', 'ZONA',
                                                                      'ZCGMUNICI', 'ZCGBAIRRO',
                                                                     'MTV_COB', 'PECLD',
                                                                      'cluster']].to_html()
            
        except Exception as e:
            print(e)


    def selecao_remover_cliente(self, b):
        """
        Remove um cliente da seleção
        :param b: instância widget
        """
        # Todo Add um try
        cc = self.remover_cliente_text.value
        mapa = self.selecao.remover_cliente(contas=[cc], plot=True)
        self.html_selecao.value = self.tabela_resultado_selecao()
        self.html_selecao.value += mapa._repr_html_()
        self.remover_cliente_text.value = ""
        self.html_selecionados.value = self.selecao.selecionados[['ZCGACCOUN', 'UTD', 'ZONA',
                                                                  'ZCGMUNICI', 'ZCGBAIRRO',
                                                                  'MTV_COB', 'PECLD',
                                                                  'cluster']].to_html()


    def selecao_exibir_link_csv(self, b):
        """
        Gera o arquivo csv da seleção e exibe o link na tab de resultados
        :param b: instância widget
        """
        self.selecao.gerar_csv()
        self.html_link_selecao_csv.value = """ 
            <a href='resultados/{0}' target='_blank'>resultados/{0}</a><br>
        """.format(self.selecao.nome_csv)


    def selecao_exibir_link_mapa(self, b):
        """
        Gera o arquivo html do mapa da seleção e exibe e o link na tab de resultados
        :param b: instância widget
        """
        self.selecao.salvar_mapa()
        self.html_link_selecao_mapa.value = """ 
            <a href='resultados/{0}' target='_blank'>resultados/{0}</a><br>
        """.format(self.selecao.nome_mapa)
   

    def append_massiva(self,b):
        """
        Salva os resultados em um arquivo de seleção massiva xlsx
        :param b: instância widget
        """
        self.botao_selecao_append.disabled = True
        
     
        try:
            if len(self.selecionados_sm.index)> 0:
                if (True in self.selecao.selecionados['ZCGACCOUN'].isin(self.selecionados_sm['ZCGACCOUN']).values):
                    self.html_link_append_arquivo.value = """ 
                                               <p>Contas Contrato Já Contidas na Seleção.</p>  
                                                """
              
                else:
                    clusterid = self.set_cluster_id()
                
                    selecionados_append = self.selecao.selecionados
                    selecionados_append[['cluster']] = selecionados_append[['cluster']].apply(lambda x: x + 1)
                    selecionados_append['cluster_id'] = clusterid
                    selecionados_append['cluster_id'] = selecionados_append['cluster_id'] +"_"+ selecionados_append['cluster'].astype(str)
                    self.selecionados_sm = self.selecionados_sm.append(selecionados_append)
                    selecao_nome = self.nome_selecao+".xlsx"
                  
                    self.selecionados_sm.to_excel('resultados/'+selecao_nome , index=False)
                    self.html_link_append_arquivo.value = """ 
                                                <a href='resultados/{0}' target='_blank'>resultados/{0}</a><br>
                                                      """.format(selecao_nome)
                    self.arquivo_hana.value = selecao_nome
                    self.tabela_resultado_sm()
            else:
                clusterid = self.set_cluster_id()
                selecionados_append = self.selecao.selecionados
                selecionados_append[['cluster']] = selecionados_append[['cluster']].apply(lambda x: x+1)
                selecionados_append ['cluster_id'] = clusterid + selecionados_append['cluster'].astype(str)
                self.selecionados_sm = self.selecionados_sm.append(selecionados_append)
                selecao_nome = self.nome_selecao+".xlsx"
                self.selecionados_sm.to_excel('resultados/'+selecao_nome , index=False)
     
                self.html_link_append_arquivo.value = """<a href='resultados/{s}' target='_blank'>resultados/{s}</a><br>
                                                      """.format(s=selecao_nome)
                self.html_link_selecao_massiva_csv.value = """ <a href='resultados/{val}' target='_blank'>Resultado da Seleção</a><br>
                                            """.format(val=selecao_nome)
                self.att_consulta_cluster_hist()
                self.arquivo_hana.value = selecao_nome
                self.tabela_resultado_sm()
        except Exception as e:
            self.html_link_append_arquivo.value = """<label>Erro no Anexo da Seleção, {e}</label>""".format(e=e)
            
        self.botao_selecao_append.disabled = False
        
    def selecao_gerar_excel(self,b):
        self.botao_selecao_gerar_excel.disabled = True
        try:
            clusterid = self.set_cluster_id()

            selecao_excel = self.selecao.selecionados.copy()
            
            selecao_excel[['cluster']] = selecao_excel[['cluster']].apply(lambda x: x+1)
            selecao_excel['clusterid'] = clusterid
            selecao_excel[['clusterid']] = selecao_excel['clusterid']  + "_" + selecao_excel['cluster'].astype(str)
            
            if self.utd:
                nome = self.utd
            elif self.zona:
                nome = nome+str(self.zona)
            else: 
                nome = clusterid
      
                
            
            
            nome_excel = "selecao_{}_{}.xlsx".format(nome,str("{:%d%m}".format(datetime.now())))

            selecao_excel.to_excel('resultados/'+nome_excel, index=False)
            self.att_consulta_cluster_hist()
            self.arquivo_hana.value = nome_excel
           
            self.html_link_append_arquivo.value = """ 
                           <a href='resultados/{0}' target='_blank'>resultados/{0}</a><br>
                            """.format(nome_excel)
        except Exception as e:
            print(e)
            
            
        self.botao_selecao_gerar_excel.disabled = False
        
    def testar_raios(self, b):
        """
        Realiza a seleção para os raios da lista de raios r_lista. Exibe os resultados dessas seleções em uma tabela
        na tab de testagem de raios.
        :param b: instância widget
        """
        self.botao_realizar_testagem.disabled = True
        self.status_parametros.value = ''
        self.html_resultado_testagem.value = ''
        self.set_r_lista(self.r_lista_text)
        self.status_testagem_r_lista.value = """Testagem em progresso: 0%<progress value="0" max="100"></progress>"""

        tabela_resultados = """<table>
                        <tr>
                            <th>CLUSTER</th>
                            <th>CLIENTES</th>
                            <th>RAIO (m)</th>
                            <th>MTVCOB</th>
                            <th>PECLD CONS</th>
                        </tr>
                      """
        try:
            for i, r in enumerate(self.r_lista):
                self.status_testagem_r_lista.value = """Testagem em progresso: {0}%<progress value="{0}" max="100"></progress>""".\
                    format(int(i*100/len(self.r_lista)))
                tabela_resultados += """<tr><td colspan=5, style='background-color:#95a63b;'>
                r = <b>{} METROS</b></td></tr>""".format(r)
                #print(self.zona)
                try:
                    resultado = self.selecao.selecionar(metodo=self.metodo, peso_mtvcob=self.peso_mtvcob,
                                               peso_pecld=self.peso_pecld,
                                               calcular_irr=self.calcular_irr,
                                               UTD=self.utd, zona=self.zona, municipio=self.municipio,
                                               bairros=self.bairros,
                                               n=self.n, k=self.k, r=r,
                                               r_max_preciso=self.r_max_preciso,
                                               min_selecionados=self.min_selecionados, local=self.local, plot=False)
                    #print(resultado)
                except IndexError:
                    self.status_testagem_r_lista.value = """<label>Não foi encontrada essa quantidade de clusters 
                        para os parâmetros indicados.</label>"""

                mtv_metro_tot = 0
                pecld_metro_tot = 0
                r_tot = 0

                for cluster in resultado:
                    if cluster[0] == 'TOTAL':
                        if r_tot ==0:
                            r_tot = 1
                        tabela_resultados += """
                                                <tr>
                                                    <td>{0}</td>
                                                    <td>{1} clientes</td>
                                                    <td>{2}</td>
                                                    <td><strong>{3} ({4}) </strong></td>
                                                    <td><strong>{5} ({6}) </strong></td>
                                                </tr>
                                             """.format(cluster[0], cluster[1], cluster[2],
                                                        Money(cluster[3],'BRA').format('es_ES'),
                                                        Money(mtv_metro_tot / r_tot,'BRA').format('es_ES'),
                                                        Money(cluster[4],'BRA').format('es_ES'),
                                                        Money(pecld_metro_tot / r_tot,'BRA').format('es_ES')
                                                        )

                    else:
                        mtv_metro = round(cluster[3] / cluster[2], 2)
                        pecld_metro = round(cluster[4] / cluster[2], 2)


                        r_tot += cluster[2]
                        if r_tot ==0:
                            r_tot = 1
                        pecld_metro_tot += cluster[4]
                        mtv_metro_tot += cluster[3]
                        #print(mtv_metro_tot)
                        #print(r_tot)
                        tabela_resultados += u"""
                                                <tr>
                                                    <td>{0}</td>
                                                    <td>{1} clientes</td>
                                                    <td>{2} metros </td>
                                                    <td>{3} ({4} por metro)</td>
                                                    <td>{5} ({6} por metro)</td>
                                                </tr>
                                             """.format(cluster[0],
                                                        cluster[1],
                                                        cluster[2],
                                                        Money(cluster[3],'BRA').format('es_ES'),
                                                        Money(mtv_metro_tot / r_tot,'BRA').format('es_ES'),
                                                        Money(cluster[4],'BRA').format('es_ES'),
                                                        Money(pecld_metro_tot / r_tot,'BRA').format('es_ES')
                                                        )

                self.html_resultado_testagem.value = tabela_resultados
                self.status_testagem_r_lista.value = """Testagem concluída: 100%<progress value="100" max="100"></progress>"""
                self.botao_realizar_testagem.disabled = False

        except Exception as e:
            self.status_testagem_r_lista.value = """<label class="status-label error-label">Não foi possível encontrar clusters para os parâmetros indicados, possível falta de suscetíveis.</label>"""
            self.botao_realizar_testagem.disabled = False
            #print(e)
            
    def pesquisar_maiores_devedores(self, b):
        """
        Pesquisa os maiores devedores de acordo com MTVCOB, PECLD e/ou QTFTVE
        :param b: instância widget
        """
        df = self.selecao.dfn.copy()
        self.nome_maiores_devedores = ""
        if self.devedores_utd_text.value:
            utd = self.set_contas_list(self.devedores_utd_text.value.upper())
            df = df.loc[df['UTD'].isin(utd)]
            self.nome_maiores_devedores = self.nome_maiores_devedores + self.devedores_utd_text.value.upper()+ "_"
        if self.devedores_zona_text.value:
            zona = self.set_contas_list(self.devedores_zona_text.value.upper())
            df = df.loc[df['ZONA'].isin(zona)]
            self.nome_maiores_devedores = self.nome_maiores_devedores + self.devedores_zona_text.value.upper()+ "_"
        if self.devedores_municipio_text.value:
            municipio = self.set_contas_list(self.devedores_municipio_text.value.upper())
            df = df.loc[df['ZCGMUNICI'].isin(municipio)]
            self.nome_maiores_devedores = self.nome_maiores_devedores + self.devedores_municipio_text.value.upper()+ "_"
        if self.devedores_bairro_text.value:
            bairro = self.set_contas_list(self.devedores_bairro_text.value.upper())
            df = df.loc[df['ZCGBAIRRO'].isin(bairro)]
            self.nome_maiores_devedores = self.nome_maiores_devedores + self.devedores_bairro_text.value.upper()+ "_"

        if self.devedores_ordenar_text.value == 'PECLD':
            df = df.sort_values('PECLD', ascending=False)
        elif self.devedores_ordenar_text.value == 'MTVCOB':
            df = df.sort_values('MTV_COB', ascending=False)
        elif self.devedores_ordenar_text.value == 'QTFTVE':
            df = df.sort_values('ZCGQTFTVE', ascending=False)
        self.nome_maiores_devedores = self.nome_maiores_devedores + self.devedores_ordenar_text.value
        
        try:
            self.df_maiores_devedores = df[['ZCGACCOUN','ZONA','UTD','MTV_COB','PECLD','LATITUDE','LONGITUDE']].head(self.devedores_qtd_text.value)
            self.html_maiores_devedores.value = df[['ZCGACCOUN', 'UTD', 'ZCGMUNICI',
                                                                           'MTV_COB', 'PECLD',
                                                                           'ZCGQTFTVE']][
                                                                       :int(self.devedores_qtd_text.value)].to_html(index=False)
        except:
            self.html_maiores_devedores.value = """<label class="status-label error-label">Não foi possível encontrar devedores para os parâmetros indicados, possível falta de suscetíveis.</label>"""
            
    def salvar_maiores_devedores_excel(self,b):
        self.botao_salvar_maiores_devedores.disabled = True
        try:
            self.df_maiores_devedores.to_excel('resultados/'+self.nome_maiores_devedores+'.xlsx', index=False)
            self.html_link_selecao_maiores_devedores_xlsx.value = """<a href='resultados/{val}.xlsx' target='_blank'>Arquivo de Resultados Para Maiores Devedores</a><br>
            """.format(val=self.nome_maiores_devedores)
            self.att_consulta_cluster_hist()
            self.arquivo_hana.value = self.nome_maiores_devedores + '.xlsx'
        except:
            pass
        self.botao_salvar_maiores_devedores.disabled = False
        
    def plotar_resultados_sm(self):
        self.html_tabela_resultados_sm.value = """<label class="status-label success-label">Seleção Concluída, Plotando Clusters...</label>"""
        mapa_sm = folium.Map(
                    location=[self.selecionados_sm['LATITUDE'].mean(), self.selecionados_sm['LONGITUDE'].mean()],
                    zoom_start=7, prefer_canvas=True
                ) 

        marker_cluster = MarkerCluster().add_to(mapa_sm)
        if not 'IRR' in self.selecao.dfn.columns or self.calcular_irr_sm_text.value == True:
            self.selecao.dfn['irr'] = (self.peso_mtv_cob_wgt.value * self.selecao.dfn['ZCGMTVCOB'] + self.peso_pecld_wgt.value * self.selecao.dfn['PECLD_CONS'])*100
        else: 
            self.selecao.dfn['irr'] = self.selecao.dfn['IRR'].copy()
        nao_selecionados = self.selecao.dfn[~self.selecao.dfn.ZCGACCOUN.isin(self.selecionados_sm['ZCGACCOUN'].tolist())].copy()
        nao_selecionados = nao_selecionados.loc[(nao_selecionados.PECLD>=self.destaque_pecld_sm)&
                                                (nao_selecionados.MTV_COB>=self.destaque_mtvcob_sm)&
                                                (nao_selecionados.ZCGQTFTVE>=self.destaque_qtftve_sm)]
        nao_selecionados = nao_selecionados.dropna(subset=['LATITUDE','LONGITUDE','irr'],  how= 'any')
        for i_ns, nao_selecionado in nao_selecionados.sort_values('irr', ascending=False)[:50].iterrows():
            folium.Marker(
                location=[nao_selecionado['LATITUDE'], nao_selecionado['LONGITUDE']],
                popup="""<strong>ACC:</strong> {0} <br> 
                                <strong>UTD:</strong> {1} <br> 
                                <strong>MUN:</strong> {2} <br> 
                                <strong>BAIRRO:</strong> {3} <br> 
                                <strong>ZONA:</strong> {4} <br> 
                                <strong>ZGCMTVCOB:</strong> R${5:.2F}<br> 
                                <strong>PECLD:</strong> R${6:.2F}<br>
                                <strong>QTFTVE:</strong> {7}<br>
                                <strong>TIPLOC:</strong> {8}<br>
                                <a target="_blank" href="https://www.google.com.br/maps/place/{9}+{10}/@{9},{10},12z">
                                google maps</a>""".format(
                    nao_selecionado['ZCGACCOUN'],
                    nao_selecionado['UTD'],
                    nao_selecionado['ZCGMUNICI'],
                    nao_selecionado['ZCGBAIRRO'],
                    nao_selecionado['ZONA'],
                    nao_selecionado['MTV_COB'],
                    nao_selecionado['PECLD'],
                    nao_selecionado['ZCGQTFTVE'],
                    nao_selecionado['ZCGTIPLOC'],
                    nao_selecionado['LATITUDE'],
                    nao_selecionado['LONGITUDE'],
                ),
                icon=folium.Icon(color='gray', icon='home')
            ).add_to(mapa_sm)

        for i, selecionado in self.selecionados_sm.iterrows():
            #selecionados_sm.loc[selecionados_sm.ZCGACCOUN==selecionado.ZCGACCOUN, 'dist'] = atena.selecao.calcular_distancia(
            #    selecionados_sm.loc[selecionados_sm['cluster_id']==selecionado.cluster_id][['LATITUDE', 'LONGITUDE']].mean(),
            #    selecionado[['LATITUDE', 'LONGITUDE']],
            #    )
            folium.Marker(
                        location=[selecionado['LATITUDE'], selecionado['LONGITUDE']],
                        popup="""<strong>ACC:</strong> {0} <br> 
                                        <strong>UTD:</strong> {1} <br> 
                                        <strong>MUN:</strong> {2} <br> 
                                        <strong>BAIRRO:</strong> {3} <br> 
                                        <strong>ZONA:</strong> {4} <br> 
                                        <strong>CLUSTER:</strong> {11} <br> 
                                        <strong>ZGCMTVCOB:</strong> R${5:.2F}<br> 
                                        <strong>PECLD:</strong> R${6:.2F}<br>
                                        <strong>QTFTVE:</strong> {7}<br>
                                        <strong>TIPLOC:</strong> {8}<br>
                                        <a target="_blank" href="https://www.google.com.br/maps/place/{9}+{10}/@{9},{10},12z">
                                        google maps</a>""".format(
                            selecionado['ZCGACCOUN'],
                            selecionado['UTD'],
                            selecionado['ZCGMUNICI'],
                            selecionado['ZCGBAIRRO'],
                            selecionado['ZONA'],
                            selecionado['MTV_COB'],
                            selecionado['PECLD'],
                            selecionado['ZCGQTFTVE'],
                            selecionado['ZCGTIPLOC'],
                            selecionado['LATITUDE'],
                            selecionado['LONGITUDE'],
                            selecionado['cluster_id']
                        ),
                        icon=folium.Icon(icon='home')
                    ).add_to(marker_cluster)
                    
        for cluster in self.selecionados_sm['cluster_id'].unique():
            folium.Circle(location=[self.selecionados_sm.loc[self.selecionados_sm['cluster_id']==cluster]['LATITUDE'].mean(), 
                                    self.selecionados_sm.loc[self.selecionados_sm['cluster_id']==cluster]['LONGITUDE'].mean()], 
                        radius=self.selecionados_sm.loc[self.selecionados_sm.cluster_id==cluster]['dist'].max(),
                        popup="""Área dentro do raio de {:.2F}m do cluster <strong>{}</strong>. <br>
                        <strong>MTVCOB:</strong> R${:.2F} <br>
                        <strong>PECLD:</strong> R${:.2F} <br>
                        <strong>Selecionados:</strong> {} <br>
                        """.format(self.selecionados_sm.loc[self.selecionados_sm.cluster_id==cluster]['dist'].max(), 
                                    cluster, 
                                    self.selecionados_sm.loc[self.selecionados_sm.cluster_id==cluster]['MTV_COB'].sum(), 
                                    self.selecionados_sm.loc[self.selecionados_sm.cluster_id==cluster]['PECLD'].sum(),
                                    self.selecionados_sm.loc[self.selecionados_sm.cluster_id==cluster]['ZCGACCOUN'].count()),
                        color='blue', 
                        fill=True, 
                        fill_color='blue').add_to(mapa_sm)   
            

        self.html_link_selecao_massiva_mapa.value = """ 
            <a href='resultados/{val}.html' target='_blank'>Mapa da Seleção</a><br>
        """.format(val=self.nome_selecao)
        
        #self.html_link_selecao_massiva_csv = widgets.HTML("""""", layout=widgets.Layout(width='50%'))
        #self.html_link_selecao_massiva_mapa = widgets.HTML("""""", layout=widgets.Layout(width='50%'))
        #self.html_link_selecao_massiva_erros = widgets.HTML("""""", layout=widgets.Layout(width='50%'))
        mapa_sm.save("resultados/"+self.nome_selecao + ".html")
        self.html_selecao_massiva.value = mapa_sm._repr_html_()
    
        
    def inserir_zonas(self,b):
        self.botao_zonas_pselecionar.disabled = True
        
        try:
            lista_zonas = self.zonas_pselecionar.value
            self.set_grupo_inserir_sm(self.drop_grupos_massiva)
            zonas_inserir = self.set_contas_list(lista_zonas)
            
            
            if self.grupo_sm == 'ZONA':
                j = 0
                for i in zonas_inserir:
                    zonas_inserir[j] = i.zfill(4)
                    j +=1
            
            for i in zonas_inserir:
                if self.grupo_sm in self.selecionados_sm:
                    if not(self.selecionados_sm[self.grupo_sm].str.contains(i).any()):
                        self.selecionados_sm = self.selecionados_sm.append(self.n_selecionados_sm.loc[self.n_selecionados_sm[self.grupo_sm] ==i])
                else:
                    self.selecionados_sm = self.selecionados_sm.append(self.n_selecionados_sm.loc[self.n_selecionados_sm[self.grupo_sm] ==i])
                                    
            self.selecionados_sm.to_excel('resultados/'+self.nome_selecao+'.xlsx', index=False)
            self.zonas_pselecionar.value = ''
            self.add_cliente_clusterid_sm.options = self.selecionados_sm['cluster_id'].unique().tolist() + ["Nenhum"] 
            self.tabela_resultado_sm()
        except Exception as e:
            print(e)
            pass
        self.botao_zonas_pselecionar.disabled = False
            
    def inserir_conta_massiva(self,b):
        self.botao_add_cc.disabled = True
        try:
            lista_contas = self.add_cliente_cc_sm_text.value
            contas_inserir = self.set_contas_list(lista_contas)
            clusterid = self.add_cliente_clusterid_sm.value
            j = 0
            for i in contas_inserir:
                contas_inserir[j] = i.zfill(12)
                j +=1
            
            for cc in contas_inserir:
                if clusterid != 'Nenhum':
                    if not(self.selecionados_sm['ZCGACCOUN'].str.contains(cc).any()):
                        conta = self.selecao.dfn.loc[self.selecao.dfn['ZCGACCOUN'] == cc]
                        conta['cluster_id'] = clusterid
                        conta['cluster'] = clusterid[-1]
                        conta['MARCACAO'] = self.selecionados_sm.loc[self.selecionados_sm['cluster_id'] == clusterid]['MARCACAO'].value_counts().idxmax()
                        try:
                            #depois refazer pra ficar com o raio certo
                            conta['RAIO'] = self.selecionados_sm.loc[self.selecionados_sm['cluster_id'] == clusterid].RAIO.max()
                        except:
                            conta['RAIO'] = 0
                        self.selecionados_sm = self.selecionados_sm.append(conta)

                else:
                    if not(self.selecionados_sm['ZCGACCOUN'].str.contains(cc).any()):
                        conta = self.selecao.dfn.loc[self.selecao.dfn['ZCGACCOUN'] == cc]
                        zona = conta.iloc[0]['ZONA'] 
                        conta['RAIO'] = 0
                        if len(self.selecionados_sm.loc[self.selecionados_sm['ZONA'] == zona].cluster.unique()) > 0:
                            conta['cluster'] = len(self.selecionados_sm.loc[self.selecionados_sm['ZONA'] == zona].cluster.unique())+1
                        else:
                            conta['cluster'] =  1
                        conta['cluster_id'] =  conta['ZONA'] + '_' + conta['cluster'].astype(str) 
                        self.selecionados_sm = self.selecionados_sm.append(conta)

                        #conta['cluster'] = self.selecionados_sm.groupby()
                            

            self.selecionados_sm.to_excel('resultados/'+self.nome_selecao+'.xlsx', index=False)
            self.add_cliente_clusterid_sm.options = self.selecionados_sm['cluster_id'].unique().tolist() + ["Nenhum"]
            self.tabela_resultado_sm()
        except Exception as e:
            print(e)
            pass
 
        self.add_cliente_cc_sm_text.value = ''    
        self.botao_add_cc.disabled = False
        
    def remover_conta_massiva(self,b):
        """
        Remove um cliente da seleção

        :param contas: lista de contas contratos dos clientes a serem removidos da seleção
        """
        # Verifica se as contas a serem removidas realmente estão contidas na seleção. ToDo tentar fazer isso com numpy
        self.botao_cc_removidas.disabled = True
        lista_contas = self.cc_removidas_massiva.value
        contas_remover = self.set_contas_list(lista_contas)
        j = 0
        for i in contas_remover:
            contas_remover[j] = i.zfill(12)
            j +=1
            try:
                #se funcionar refazer para não precisar utilizar o loop
                
                for cc in contas_remover:
                    if (cc in self.selecionados_sm['ZCGACCOUN'].tolist()):
                        self.selecionados_sm = self.selecionados_sm.loc[~self.selecionados_sm['ZCGACCOUN'].isin([cc])]
                self.selecionados_sm.to_excel('resultados/'+self.nome_selecao+'.xlsx', index=False)
                self.add_cliente_clusterid_sm.options = self.selecionados_sm['cluster_id'].unique().tolist() + ["Nenhum"]
                self.tabela_resultado_sm()
            except:
                pass
   
        self.cc_removidas_massiva.value = ''    
        self.botao_cc_removidas.disabled = False    
            
            
    def atualizar_mapa(self,b):
        self.botao_atualizar_mapa.disabled = True
        try:
            self.plotar_resultados_sm()
            self.tabela_resultado_sm()
        except Exception as e:
            print(e)
        self.botao_atualizar_mapa.disabled = False
    
    def on_upload_sm_change(self,change):
        if not change.new:
            return
        else:
            self.importar_conf_sm_text.value = False
            self.arquivo_conf_zonas = self.botao_sm_arquivo.value
            self.botao_sm_arquivo._counter = 1
   
    
            
            
            
    def realizar_selecao_massiva(self, b):
        
        try: 
            self.set_metodo_sm(self.metodo_sm_text)
            self.set_calcular_irr_sm(self.calcular_irr_sm_text) 
            self.set_r_max_preciso_sm(self.r_max_preciso_sm_text) 
            self.set_importar_conf_sm(self.importar_conf_sm_text)
            self.set_destaque_pecld_sm(self.destaque_pecld_sm_text) 
            self.set_destaque_mtvcob_sm(self.destaque_mtvcob_sm_text) 
            self.set_destaque_qtftve_sm(self.destaque_qtftve_sm_text)
            self.set_plotar_clusters_sm(self.plotar_clusters_sm_text)
            self.set_multiprocess_sm(self.multiprocess_sm_text)

            self.html_tabela_resultados_sm.value = """"""
            self.html_link_selecao_massiva_erros.value = """"""
            self.html_link_selecao_massiva_mapa.value = """"""
            self.html_link_selecao_massiva_csv.value = """"""

            n_processos = os.cpu_count()-1
            self.nome_selecao = self.nome_arquivo.value



            self.botao_sm_arquivo._counter = 0



            self.selecionados_sm = pd.DataFrame()
            self.n_selecionados_sm = pd.DataFrame()
            self.erros_sm = pd.DataFrame()


            if (not self.importar_conf_sm):
                if not len(self.arquivo_conf_zonas):
                    self.status_parametros_sm.value = """<label class="status-label error-label">É necessário o Upload do arquivo de configuração    das zonas.</label>"""
                    return
                else:
                    csv = self.arquivo_conf_zonas[list(self.arquivo_conf_zonas.keys())[0]]['content']
                    zonas_sm = pd.read_csv(BytesIO(csv), delimiter=';', 
                                           converters={'ZONA':str,'NUM_CORTES_MIN':int, 'NUM_CORTES_IDEAL':int, 
                                           'CLUSTERS':int, 'RAIO_IDEAL':int, 
                                           'RAIO_MAX':int, 'RAIO_STEP':int,
                                           }, keep_default_na=False, decimal=',')
                    #resolver
                    self.status_parametros_sm.value = """"""

                    #zonas_sm['ZONA'] = zonas_sm['ZONA'].apply(lambda x: x.zfill(4) if x != "" else x)
            else:
                try:
                    zonas_sm = self.conf_sm
                    self.status_parametros_sm.value = """"""
                except:
                    self.status_parametros_sm.value = """<label class="status-label error-label">É necessário importar o arquivo de configuração das zonas.</label>"""
                    return


            self.html_tabela_resultados_sm.value = """<label class="status-label warning-label">Seleção em andamento...</label>"""

            self.selecionados_sm = pd.DataFrame()
            self.n_selecionados_sm = pd.DataFrame()
            self.erros_sm = pd.DataFrame()

            try: 

                for i in zonas_sm.CARTEIRA.unique():
                    selecionados_sm = pd.DataFrame()
                    n_selecionados_sm = pd.DataFrame()

                    if self.multi_process_sm:
                        #MULTIPROCESSAMENTO:
                        metodo = []
                        calcularirr = []
                        r_max_preciso = []

                        #CASO O ARQUIVO DE CONFG TENHA MENOS QUE 5 LINHAS SERÁ UTILIZADA O NÚMERO DE LINHAS PARA A POOL DE PROCESSOS.

                        if n_processos > len(zonas_sm.index):      
                            n_processos = len(zonas_sm.index)

                        p = ProcessPool(n_processos)
                        #zonas_sm = zonas_sm.sample(frac=1)
                        divididos = np.array_split(zonas_sm.loc[zonas_sm['CARTEIRA']==i].sample(frac=1),n_processos)
                        metodo = [self.metodo_sm]*n_processos
                        calcularirr = [self.calcular_irr_sm]*n_processos
                        r_max_preciso = [self.r_max_preciso_sm]*n_processos

                        res = list(p.imap(self.selecao_m.multiprocess_zonas, divididos,metodo,calcularirr,r_max_preciso))

                        for i in range(len(res)):
                            selecionados_sm = selecionados_sm.append(res[i][0])
                            n_selecionados_sm = n_selecionados_sm.append(res[i][1])
                            #self.selecionados_sm = self.selecionados_sm.append(res[i][0])
                            #self.n_selecionados_sm  = self.n_selecionados_sm.append(res[i][1])
                            self.erros_sm = self.erros_sm.append(res[i][2])

                            if selecionados_sm.shape[0] > 0:  
                                for j in selecionados_sm.MARCACAO.unique():
                                    self.marc_iter = self.marc_iter + 1
                                    selecionados_sm.loc[selecionados_sm['MARCACAO'] == j,'MARCACAO'] = selecionados_sm.loc[selecionados_sm['MARCACAO']==j]['MARC_ZONA'] + str(self.marc_iter).zfill(4)

                            if n_selecionados_sm.shape[0] >0: 
                                for j in n_selecionados_sm.MARCACAO.unique():
                                    self.marc_iter = self.marc_iter + 1
                                    n_selecionados_sm.loc[n_selecionados_sm['MARCACAO'] == j,'MARCACAO'] = n_selecionados_sm.loc[n_selecionados_sm['MARCACAO']==j]['MARC_ZONA'] + str(self.marc_iter).zfill(4) 

                        #self.selecao_m.contasselecionadas= self.selecao_m.contasselecionadas.append(selecionados_sm.ZCGACCOUN)
                        #self.selecao_m.contasselecionadas = self.selecao_m.contasselecionadas.append(n_selecionados_sm.ZCGACCOUN)
                        self.teste = res    
                    else:   
                        selecionados_sm,n_selecionados_sm,erros_sm, erro = self.selecao.multiprocess_zonas(zonas_sm,self.metodo_sm,self.calcular_irr_sm,self.r_max_preciso_sm)
                        self.erros_sm = self.erros_sm.append(erros_sm)

                        if selecionados_sm.shape[0] > 0:  
                            for j in selecionados_sm.MARCACAO.unique():
                                self.marc_iter = self.marc_iter + 1
                                selecionados_sm.loc[selecionados_sm['MARCACAO'] == j,'MARCACAO'] = selecionados_sm.loc[selecionados_sm['MARCACAO']==j]['MARC_ZONA'] + str(self.marc_iter).zfill(4)

                        if n_selecionados_sm.shape[0] >0: 
                            for j in n_selecionados_sm.MARCACAO.unique():
                                self.marc_iter = self.marc_iter + 1
                                n_selecionados_sm.loc[n_selecionados_sm['MARCACAO'] == j,'MARCACAO'] = n_selecionados_sm.loc[n_selecionados_sm['MARCACAO']==j]['MARC_ZONA'] + str(self.marc_iter).zfill(4) 

                        #self.selecao.contasselecionadas= self.selecao.contasselecionadas.append(selecionados_sm.ZCGACCOUN)
                        #self.selecao.contasselecionadas = self.selecao.contasselecionadas.append(n_selecionados_sm.ZCGACCOUN)

                    self.selecionados_sm = self.selecionados_sm.append(selecionados_sm)
                    self.n_selecionados_sm = self.n_selecionados_sm.append(n_selecionados_sm)

                #self.selecao.contasselecionadas = pd.DataFrame()
                #self.selecao_m.contasselecionadas = pd.DataFrame()


                self.html_tabela_resultados_sm.value = """<label class="status-label success-label">Seleção Concluída, Computando Resultados...</label>"""


                #self.selecionados_sm = self.selecionados_sm.append(selecionados_sm)
                #self.n_selecionados_sm  = self.n_selecionados_sm.append(n_selecionados_sm)                    




                if self.erros_sm.shape[0] > 0:
                    if 'cluster_id' in self.erros_sm.columns:
                        self.erros_sm = self.erros_sm [['PESO_MTVCOB','PESO_PECLD','PESO_QTDFTVE','MIN_QTDFTVE','LOCALI','MUNICIPIO','BAIRRO','TIPO_LOCAL','SERVICO','SELECIONAR','UTD','ZONA','RAIO_IDEAL','RAIO_MAX','RAIO_STEP','CARTEIRA','TURMA','CLUSTERS','NUM_CORTES_IDEAL','NUM_CORTES_MIN','QTD SELECIONAVEL CLUSTER','QTD SELECIONAVEL','cluster_id']]
                    else:
                        self.erros_sm = self.erros_sm [['UTD','SELECIONAR','ZONA','LOCALI','MUNICIPIO','BAIRRO','TIPO_LOCAL',    'RAIO_IDEAL','RAIO_MAX','RAIO_STEP','CLUSTERS','NUM_CORTES_IDEAL','NUM_CORTES_MIN','PESO_MTVCOB','PESO_PECLD','PESO_QTDFTVE','MIN_QTDFTVE','CARTEIRA','TURMA','SERVICO']]
                if  self.selecionados_sm.shape[0]> 0:
                    #self.selecionados_sm = self.selecionados_sm.drop(columns=['MARC_ZONA']) 


                    self.selecionados_sm.to_excel('resultados/'+self.nome_selecao+'.xlsx', index=False)
                    self.html_link_selecao_massiva_csv.value = """ <a href='resultados/{val}.xlsx' target='_blank'>Resultado da Seleção</a><br>
                                                    """.format(val=self.nome_selecao)

                    self.add_cliente_clusterid_sm.options = self.selecionados_sm['cluster_id'].unique().tolist() + ["Nenhum"]
                    self.att_consulta_cluster_hist()
                    self.arquivo_hana.value = self.nome_selecao + ".xlsx"

                if self.erros_sm.shape[0] > 0:
                    #if 'MARC_ZONA' in self.n_selecionados_sm:
                        #self.n_selecionados_sm = self.n_selecionados_sm.drop(columns=['MARC_ZONA'])  

                    self.erros_sm.to_excel('resultados/erros_selecao_'+self.nome_selecao+'.xlsx', index=False) 
                    self.html_link_selecao_massiva_erros.value = """ <a href='resultados/erros_selecao_{val}.xlsx' target='_blank'>Erros da Seleção</a><br>""".format(val=self.nome_selecao)
                try:
                    if self.plotar_clusters:
                        self.plotar_resultados_sm()
                    self.tabela_resultado_sm()
                except Exception as e: 
                    self.html_tabela_resultados_sm.value = """<label class="status-label error-label">Não foi possível selecionar nenhum cliente {e}.</label>""".format(e = e)
            except Exception as e:
                print("Ocorreu um erro: ", e)
        except Exception as e:
            self.status_parametros_sm.value = """<label class="status-label error-label">Ocorreu um erro na seleção, cheque o arquivo de configuração, {e}.</label>""".format(e=e)
        
    #  -------------- Fim: Funções auxiliares das tabs da interface -----------------

    #  -------------- Inicio: Tabs da interface -----------------
    def parametros_selecao(self):
        self.status_parametros = widgets.HTML("""""")
        # UTD
        label_utd = widgets.Label('UTD:', layout=widgets.Layout(width='35%'))
        self.utd = self.selecao.dfn['UTD'].sort_values().unique()[0]
        self.utd_text = widgets.Dropdown(options=self.selecao.dfn['UTD'].sort_values().unique().tolist()+ [""],
                                         layout=widgets.Layout(width="20%", height='30px'),
                                         value=self.utd)
        self.utd_text.observe(self.utd_text_change)
        container_utd = widgets.HBox(children=[label_utd, self.utd_text])

        # zona
        label_zona = widgets.Label('Zona:', layout=widgets.Layout(width='35%'))
        self.zona_text = widgets.Dropdown(options=[' '] + \
                                             sorted(
                                                 self.selecao.dfn[self.selecao.dfn.UTD == self.utd]
                                                 ['ZONA'].unique()
                                             ),
                                           layout=widgets.Layout(width="20%", height='30px'),
                                           value=self.zona)
        self.zona_text.observe(self.zona_text_change)
        container_zona = widgets.HBox(children=[label_zona, self.zona_text])
        
        #localidade
        label_locali = widgets.Label('Localidade:',layout =widgets.Layout(width='35%'))
        self.locali_text = widgets.Text(layout=widgets.Layout(width="20%", height='40px'),
                                         value=str(self.locali).replace("'", "").replace("[", "").replace("]",""))
        container_locali = widgets.HBox(children = [label_locali,self.locali_text])
        # municipio
        label_municipio = widgets.Label('Município:', layout=widgets.Layout(width='35%'))
        self.municipio_text = widgets.Dropdown(options=[" "] + list(self.selecao.dfn['ZCGMUNICI'].sort_values().unique()),
                                           layout=widgets.Layout(width="20%", height='30px'),
                                           value=self.municipio)
        container_municipio = widgets.HBox(children=[label_municipio, self.municipio_text])

        # bairros
        label_bairros = widgets.Label('Bairros:', layout=widgets.Layout(width='35%'))
        self.bairros_text = widgets.Text(layout=widgets.Layout(width="20%", height='40px'),
                                         value=str(self.bairros).replace("'", "").replace("[", "").replace("]",""))
        container_bairros = widgets.HBox(children=[label_bairros, self.bairros_text])

        # metodo
        label_metodo = widgets.Label('Método de seleção:', layout=widgets.Layout(width='35%', height='45px'))
        self.metodo_text = widgets.Dropdown(layout=widgets.Layout(width="20%", height='30px'),
                                            value=self.metodo,
                                            options=['nkcnk','fast nkcnk']
                                            )
        container_metodo = widgets.HBox(children=[label_metodo, self.metodo_text])

        # tipo local
        label_local = widgets.Label('Tipo da localização:', layout=widgets.Layout(width='35%', height='45px'))
        self.local_text = widgets.Dropdown(layout=widgets.Layout(width="20%", height='30px'),
                                            value=self.local,
                                            options=[('Rural e Urbana' , None), ('Urbana', 'U'), ('Rural', 'R')]
                                            )
        container_local = widgets.HBox(children=[label_local, self.local_text])

        # peso pecld
        label_peso_pecld = widgets.Label('Peso da PECLD:', layout=widgets.Layout(width='35%'))
        self.peso_pecld_text = widgets.Text(layout=widgets.Layout(width="20%", height='40px'),
                                            value=str(self.peso_pecld))
        container_peso_pecld = widgets.HBox(children=[label_peso_pecld, self.peso_pecld_text])

        # peso mtvcob
        label_peso_mtvcob = widgets.Label('Peso do MTVCOB:', layout=widgets.Layout(width='35%'))
        self.peso_mtvcob_text = widgets.Text(layout=widgets.Layout(width="20%", height='40px'),
                                             value=str(self.peso_mtvcob))
        container_peso_mtvcob = widgets.HBox(children=[label_peso_mtvcob, self.peso_mtvcob_text])
        #self.peso_qtftve_text
        label_peso_qtftve = widgets.Label('Peso da QTFTVE:', layout=widgets.Layout(width='35%'))
        self.peso_qtftve_text = widgets.Text(layout=widgets.Layout(width="20%", height='40px'),
                                             value=str(self.peso_qtftve))
        container_peso_qtftve = widgets.HBox(children=[label_peso_qtftve, self.peso_qtftve_text])
        # r max preciso
        self.calcular_irr_text = widgets.Checkbox(layout=widgets.Layout(width="50%", height='40px'),
                                                description="Calcular IRR", indent=False,
                                                value=self.calcular_irr)
        container_calcular_irr = widgets.HBox(children=[self.calcular_irr_text])

        # n
        label_n = widgets.Label('Número de clusters (n):', layout=widgets.Layout(width='35%'))
        self.n_text = widgets.Text(layout=widgets.Layout(width="20%", height='40px'), value=str(self.n))
        container_n = widgets.HBox(children=[label_n, self.n_text])

        # k
        label_k = widgets.Label('Número máximo de clientes em um cluster (k):', layout=widgets.Layout(width='35%'))
        self.k_text = widgets.Text(layout=widgets.Layout(width="20%", height='40px'), value=str(self.k))
        container_k = widgets.HBox(children=[label_k, self.k_text])

        # min_selecionados
        label_min_selecionados = widgets.Label('Número mínimo desejado de clientes em um cluster:',
                                               layout=widgets.Layout(width='35%'))
        self.min_selecionados_text = widgets.Text(layout=widgets.Layout(width="20%", height='40px'),
                                                  value=str(self.min_selecionados))
        container_min_selecionados = widgets.HBox(children=[label_min_selecionados, self.min_selecionados_text])

        # r max preciso
        self.r_max_preciso_text = widgets.Checkbox(layout=widgets.Layout(width="50%", height='40px'),
                                                description=" Precisão do raio máximo dos cluster", indent=False,
                                                value=self.r_max_preciso)
        container_r_max_preciso = widgets.HBox(children=[self.r_max_preciso_text])
        
        botao_salvar_parametros = widgets.Button(description="Salvar",
                                                 button_style='success',
                                                 layout=widgets.Layout(height='35px', width='99%', size='20'))

        botao_salvar_parametros.on_click(self.salvar_parametros)

        param_selecao_box = widgets.VBox(children=[
            self.status_parametros,
            container_utd,
            container_zona,
            container_locali,
            container_municipio,
            container_bairros,
            container_local,
            container_metodo,
            container_peso_pecld,
            container_peso_mtvcob,
            container_peso_qtftve,
            container_calcular_irr,
            container_n,
            container_k,
            container_min_selecionados,
            container_r_max_preciso,
            botao_salvar_parametros
        ])

        return param_selecao_box


    def testagem_r(self):
        # Lista de raios
        label_r_lista = widgets.Label('Lista de raios a serem testados:', layout=widgets.Layout(width='20%'))

        self.r_lista_text = widgets.Text(layout=widgets.Layout(width="80%", height='45px'),
                                    value=str(self.r_lista).replace("'", "").replace("[", "").replace("]",""))  # ToDo melhorar
        # r_lista_text.on_submit(self.set_r_lista)
        container_r_lista = widgets.HBox(children=[label_r_lista, self.r_lista_text])

        self.botao_realizar_testagem = widgets.Button(description="Realizar testagem",
                                                 button_style='success',
                                                 layout=widgets.Layout(height='35px', width='99.4%', size='20'))
        self.botao_realizar_testagem.on_click(self.testar_raios)

        self.status_testagem_r_lista = widgets.HTML("""""")
        self.html_resultado_testagem.value = """"""

        testagem_r_lista_box = widgets.VBox(children=[container_r_lista,
                                                      self.botao_realizar_testagem,
                                                      self.status_testagem_r_lista,
                                                      self.html_resultado_testagem])

        return testagem_r_lista_box


    def selecionar(self):
        self.status_selecao = widgets.HTML("""""")

        # r
        # label_r = widgets.Label("Raio de clusterização:", layout=widgets.Layout(width = '20%'))

        self.r_text = widgets.Text(layout=widgets.Layout(width="40%", height='45px'),
                                   description="r :", value=str(self.r))
        # r_text.on_submit(self.set_r)

        self.botao_realizar_selecao = widgets.Button(description="Realizar seleção",
                                                button_style='success',
                                                layout=widgets.Layout(height='28px', width='30%', size='20'))

        self.botao_realizar_selecao.on_click(self.realizar_selecao)
        container_r = widgets.HBox(children=[self.r_text, self.botao_realizar_selecao])

        # Edição da seleção
        label_editar_selecao = widgets.HTML("""Editar seleção""",
                                            layout=widgets.Layout(height='45px', width='90%', size='20'))

        # Add cliente
        label_add_cliente = widgets.Label("Adicionar cliente à seleção", layout=widgets.Layout(width='20%'))

        self.add_cliente_cc_text = widgets.Text(layout=widgets.Layout(width="20%", height='45px'),
                                                description="CC")
        self.add_cliente_cluster_text = widgets.Dropdown(layout=widgets.Layout(width="20%", height='30px'),
                                                     description="Cluster",
                                                     options=[''])

        botao_add_cliente = widgets.Button(description="Adicionar",
                                           button_style='info',
                                           layout=widgets.Layout(height='28px', width='30%', size='20'))
        botao_add_cliente.on_click(self.selecao_add_cliente)

        container_add_cliente = widgets.HBox(children=[self.add_cliente_cc_text, self.add_cliente_cluster_text,
                                                       botao_add_cliente])

        # trocar cluster de cliente
        label_set_cliente_cluster = widgets.Label("Trocar cluster de clienter selecionado",
                                                  layout=widgets.Layout(width='50%'))

        self.set_cliente_cluster_cc_text = widgets.Text(layout=widgets.Layout(width="20%", height='45px'),
                                                        description="CC")
        self.set_cliente_cluster_cluster_text = widgets.Dropdown(layout=widgets.Layout(width="20%", height='30px'),
                                                     description="Cluster",
                                                     options=[''])

        botao_set_cliente_cluster = widgets.Button(description="Trocar",
                                                   button_style='warning',
                                                   layout=widgets.Layout(height='28px', width='30%', size='20'))

        botao_set_cliente_cluster.on_click(self.selecao_set_cliente_cluster)

        container_set_cliente_cluster = widgets.HBox(children=[self.set_cliente_cluster_cc_text,
                                                               self.set_cliente_cluster_cluster_text,
                                                               botao_set_cliente_cluster])

        # Remover cliente
        label_remover_cliente = widgets.Label("Remover cliente da seleção:", layout=widgets.Layout(width='20%'))

        self.remover_cliente_text = widgets.Text(layout=widgets.Layout(width="40%", height='45px'),
                                                 description="CC")

        botao_remover_cliente = widgets.Button(description="Remover",
                                               button_style='danger',
                                               layout=widgets.Layout(height='28px', width='30%', size='20'))

        botao_remover_cliente.on_click(self.selecao_remover_cliente)

        container_remover_cliente = widgets.HBox(children=[self.remover_cliente_text,
                                                           botao_remover_cliente])

        selecao_box = widgets.VBox(children=[self.status_selecao,
                                             container_r,
                                             label_add_cliente,
                                             container_add_cliente,
                                             label_set_cliente_cluster,
                                             container_set_cliente_cluster,
                                             label_remover_cliente,
                                             container_remover_cliente,
                                             self.html_selecao])

        return selecao_box


    def selecao_resultados(self):

        botao_selecao_gerar_csv = widgets.Button(description="Baixar CSV",
                                                   button_style='success',
                                                   layout=widgets.Layout(height='28px', width='50%', size='20'))
        botao_selecao_gerar_csv.on_click(self.selecao_exibir_link_csv)
       
        botao_selecao_gerar_mapa = widgets.Button(description="Baixar mapa",
                                                 button_style='info',
                                                 layout=widgets.Layout(height='28px', width='50%', size='20'))
        
        
        self.botao_selecao_gerar_excel= widgets.Button(description="Baixar XLSX",
                                                   button_style='success',
                                                   layout=widgets.Layout(height='28px', width='50%', size='20'))
        
        self.botao_selecao_append = widgets.Button(description = "Inserir seleção na Massiva",
                                              button_style = 'info',
                                              layout= widgets.Layout(height = '28px', width = '50%',size = '20'))
        
        self.botao_selecao_gerar_excel.on_click(self.selecao_gerar_excel)
        self.botao_selecao_append.on_click(self.append_massiva)
        container_inserir_arquivo = widgets.HBox(children = [self.botao_selecao_gerar_excel,
                                                                 self.botao_selecao_append])
        container_botoes = widgets.HBox(children=[botao_selecao_gerar_csv,
                                                           botao_selecao_gerar_mapa])
        container_botoes_final = widgets.VBox(children = [container_botoes,
                                             container_inserir_arquivo])
        botao_selecao_gerar_mapa.on_click(self.selecao_exibir_link_mapa)
        


        
    

        self.html_link_selecao_csv = widgets.HTML("""""", layout=widgets.Layout(width='50%'))
        self.html_link_selecao_mapa = widgets.HTML("""""", layout=widgets.Layout(width='50%'))
        self.html_link_append_arquivo = widgets.HTML("""""", layout=widgets.Layout(width='50%'))


        container_links = widgets.HBox(children=[self.html_link_selecao_csv,
                                                 self.html_link_selecao_mapa,
                                                self.html_link_append_arquivo])

        label_selecao_resultados = widgets.HTML("""Clientes selecionados""",
                                                layout=widgets.Layout(height='45px', width='90%', size='20'))

        self.html_selecionados = widgets.HTML("""""")

        # Label da tab de parametros da seleção
        self.status_parametros.value = ""
        # r
        # label_r = widgets.Label("Raio de clusterização:", layout=widgets.Layout(width = '20%'))

        # self.r_text = widgets.Text(layout=widgets.Layout(width = "40%",height = '45px'),
        # description="r :", value=str(self.r))
        # r_text.on_submit(self.set_r)

        # botao_realizar_selecao = widgets.Button(description="Realizar seleção",
        # button_style = 'success',
        # layout = widgets.Layout(height = '28px', width = '30%', size = '20'))

        selecao_resultados_box = widgets.VBox(children=[container_botoes_final,
                                                        container_links,
                                                        label_selecao_resultados, self.html_selecionados])

        return selecao_resultados_box


    def maiores_devedores(self):
        # Ordenar por
        self.html_link_selecao_maiores_devedores_xlsx = widgets.HTML("""""")
        label_devedores_ordenar = widgets.Label('Ordenar por:', layout=widgets.Layout(width='15%', height='45px'))
        self.devedores_ordenar_text = widgets.Dropdown(layout=widgets.Layout(width="25%", height='30px'),
                                                       value='PECLD',
                                                       options=['QTFTVE', 'PECLD', 'MTVCOB'])
        container_devedores_ordenar = widgets.HBox(children=[label_devedores_ordenar, self.devedores_ordenar_text])

        # Qtd de Devedores
        label_devedores_qtd = widgets.Label('Qtd. de devedores:', layout=widgets.Layout(width='15%'))
        self.devedores_qtd_text = widgets.IntText(layout=widgets.Layout(width="25%", height='40px'), value='50')
        container_devedores_qtd = widgets.HBox(children=[label_devedores_qtd, self.devedores_qtd_text])

        # UTD
        label_devedores_utd = widgets.Label('UTD:', layout=widgets.Layout(width='15%'))
        self.devedores_utd_text = widgets.Text(layout=widgets.Layout(width="25%", height='40px'))
        container_devedores_utd = widgets.HBox(children=[label_devedores_utd, self.devedores_utd_text])

        # zona
        label_devedores_zona = widgets.Label('Zona:', layout=widgets.Layout(width='15%'))
        self.devedores_zona_text = widgets.Text(layout=widgets.Layout(width="25%", height='40px'))
        container_devedores_zona = widgets.HBox(children=[label_devedores_zona, self.devedores_zona_text])

        # municipio
        label_devedores_municipio = widgets.Label('Município:', layout=widgets.Layout(width='15%'))
        self.devedores_municipio_text = widgets.Text(layout=widgets.Layout(width="25%", height='40px'))
        container_devedores_municipio = widgets.HBox(children=[label_devedores_municipio,
                                                               self.devedores_municipio_text])

        # bairros
        label_devedores_bairro = widgets.Label('Bairro:', layout=widgets.Layout(width='15%'))
        self.devedores_bairro_text = widgets.Text(layout=widgets.Layout(width="25%", height='40px'))
        container_devedores_bairro = widgets.HBox(children=[label_devedores_bairro,
                                                            self.devedores_bairro_text])

        self.html_maiores_devedores = widgets.HTML("""""")

        botao_pesquisar_maiores_devedores = widgets.Button(description="Pesquisar",
                                                           button_style='success',
                                                           layout=widgets.Layout(height='35px', width='50%', size='20'))
        botao_pesquisar_maiores_devedores.on_click(self.pesquisar_maiores_devedores)
        self.botao_salvar_maiores_devedores = widgets.Button(description = "Salvar",
                                                        button_style = 'info',
                                                        layout=widgets.Layout(height='35px', width='50%', size='20')
                                                        )
        self.botao_salvar_maiores_devedores.on_click(self.salvar_maiores_devedores_excel)
        
        container_botoes = widgets.HBox(children = [botao_pesquisar_maiores_devedores,self.botao_salvar_maiores_devedores])
        maiores_devedores_box = widgets.VBox(children=[container_devedores_ordenar,
                                                       container_devedores_qtd,
                                                       container_devedores_utd,
                                                       container_devedores_zona,
                                                       container_devedores_municipio,
                                                       container_devedores_bairro,
                                                       container_botoes,
                                                       self.html_link_selecao_maiores_devedores_xlsx,
                                                       self.html_maiores_devedores], layout=widgets.Layout(min_height='500px'))

        return maiores_devedores_box
    
    # Selecao Massiva: SM
    def selecao_massiva(self):
        self.status_parametros_sm = widgets.HTML("""""")
        self.html_link_selecao_massiva_csv = widgets.HTML("""""", layout=widgets.Layout(width='33%'))
        self.html_link_selecao_massiva_mapa = widgets.HTML("""""", layout=widgets.Layout(width='33%'))
        self.html_link_selecao_massiva_erros = widgets.HTML("""""", layout=widgets.Layout(width='33%'))
        container_links_arquivos = widgets.HBox(children=[self.html_link_selecao_massiva_csv,
                                                self.html_link_selecao_massiva_mapa,
                                                self.html_link_selecao_massiva_erros])
        self.html_tabela_resultados_sm = widgets.HTML("""""", layout = widgets.Layout(width = '95%', margin = "20px 0px 0px 20px"))

        # Escolher Arquivo
        label_sm_arquivo = widgets.Label('Configuração da seleção:', layout=widgets.Layout(width='170px', height='45px'))
        self.botao_sm_arquivo = widgets.FileUpload(accept='.csv,.xls', multiple=False)
        self.botao_sm_arquivo.observe(self.on_upload_sm_change)
        #container_sm_arquivo = widgets.HBox(children=[label_sm_arquivo, self.botao_sm_arquivo])
         
        # metodo
        label_metodo_sm = widgets.Label('Método:', layout=widgets.Layout(width='120x', height='45px', margin="0px 0px 0px 0px"))
        self.metodo_sm_text = widgets.Dropdown(layout=widgets.Layout(width="100px", height='30px', margin="0px 0px 0px 0px"),
                                                             value=self.metodo_sm,
                                                             options=['nkcnk','fast nkcnk']
                                                             )
        label_nome_arquivo = widgets.Label('Nome do Arquivo de Saída:',layout=widgets.Layout(width='350x', height='45px', margin="0px 0px 0px 0px"))
        
        self.nome_arquivo = widgets.Text(value = self.consulta_nome+"_"+str("{:%d%m}".format(datetime.now())), disabled = False,      layout=widgets.Layout(width="800x", height='30px', margin="0px 40px 0px 20px"))
        #container_metodo_sm = widgets.HBox(children=[label_metodo_sm, self.metodo_sm_text])

        # calcular irr
        self.calcular_irr_sm_text = widgets.Checkbox(layout=widgets.Layout(width="115px", height='40px'),
                                                description="Calcular IRR", indent=False,
                                                value=self.calcular_irr_sm)

        # r max preciso
        self.r_max_preciso_sm_text = widgets.Checkbox(layout=widgets.Layout(width="150px", height='40px'),
                                                description=" Precisão do raio", indent=False,
                                                value=self.r_max_preciso_sm)
        self.importar_conf_sm_text = widgets.Checkbox(layout=widgets.Layout(width="180px", height='40px'),
                                                description=" Importar Configuração", indent=False,
                                                value=self.importar_conf_sm)
                                                                                        
        self.plotar_clusters_sm_text = widgets.Checkbox(layout=widgets.Layout(width="180px", height='40px'),
                                                description="Plotar Clusters", indent=False,
                                                value=self.plotar_clusters_sm)
        self.multiprocess_sm_text = widgets.Checkbox(layout=widgets.Layout(width="200px", height='40px'),
                                                description="Utilizar Multiprocessamento", indent=False,
                                                value=self.multi_process_sm)
        
        container_parametros_sm = widgets.HBox(children=[label_sm_arquivo, self.botao_sm_arquivo,
                                                        label_metodo_sm, self.metodo_sm_text, 
                                                        self.importar_conf_sm_text,
                                                        self.calcular_irr_sm_text,
                                                        self.r_max_preciso_sm_text]
                                                         )
        container_nome_arquivo = widgets.HBox(children =[label_nome_arquivo,self.nome_arquivo,
                                                         self.plotar_clusters_sm_text,
                                                        self.multiprocess_sm_text ])
         # Detaque PECL
        label_destaque_pecld_sm = widgets.Label('Destaque PECLD:', layout=widgets.Layout(width='120px', height='45px', margin="0px 0px 0px 0px"))
        self.destaque_pecld_sm_text = widgets.IntText(layout=widgets.Layout(width="80px", height='30px', margin="0px 40px 0px 0px"),
                                                             value=self.destaque_pecld_sm
                                                             )

        # Detaque MTVCOB
        label_destaque_mtvcob_sm = widgets.Label('Destaque MTVCOB:', layout=widgets.Layout(width='120px', height='45px', margin="0px 0px 0px 0px"))
        self.destaque_mtvcob_sm_text = widgets.IntText(layout=widgets.Layout(width="80px", height='30px', margin="0px 40px 0px 0px"),
                                                             value=self.destaque_mtvcob_sm
                                                             )

        # Detaque QTFTVE
        label_destaque_qtftve_sm = widgets.Label('Destaque QTFTVE:', layout=widgets.Layout(width='120px', height='45px', margin="0px 0px 0px 0px"))
        self.destaque_qtftve_sm_text = widgets.IntText(layout=widgets.Layout(width="80px", height='30px', margin="0px 40px 0px 0px"),
                                                             value=self.destaque_qtftve_sm
                                                             )
        container_parametros_destaque_sm = widgets.HBox(children=[label_destaque_pecld_sm, self.destaque_pecld_sm_text,
                                                        label_destaque_mtvcob_sm, self.destaque_mtvcob_sm_text, 
                                                        label_destaque_qtftve_sm, self.destaque_qtftve_sm_text])
        
        # botao realizar selecao
        botao_realizar_sm = widgets.Button(description="Realizar Seleção Massiva",
                                               button_style='success',
                                               layout=widgets.Layout(height='35px', width='99.4%', size='20'))
        
        botao_realizar_sm.on_click(self.realizar_selecao_massiva)
        
        #adicionar clientes ou zonas a seleção
        label_zonas_pselecionar = widgets.Label('Inserir Grupo na Seleção:', layout=widgets.Layout(width='250px', height='50px'))
        self.drop_grupos_massiva = widgets.Dropdown(options=self.opcoes_grupos_sm,value=self.grupo_sm,layout=widgets.Layout(width='120px', height='28px', margin="0px 40px 0px 0px"))
        self.zonas_pselecionar = widgets.Text(layout=widgets.Layout(width="200px", height='30px', margin="0px 40px 0px 0px"),
                                                             value='')
        self.botao_zonas_pselecionar = widgets.Button(description="Inserir",
                                               button_style='info',
                                               layout=widgets.Layout(width="120px", height='30px', margin="0px 0px 0px 0px"))
        
        label_add_cliente_cc_sm = widgets.Label('Inserir Contas na Seleção:', layout=widgets.Layout(width='250px', height='50px', margin="0px 0px 0px 0px"))
        self.add_cliente_cc_sm_text = widgets.Text(layout=widgets.Layout(width="200px", height='30px', margin="0px 40px 0px 0px"),
                                                             value='')
        label_add_cliente_clusterid = widgets.Label('Cluster ID:', layout=widgets.Layout(width='100px', height='50px', margin="0px 0px 0px 0px"))
        self.add_cliente_clusterid_sm = widgets.Dropdown(layout=widgets.Layout(width="200px", height='28px', margin="0px 40px 0px 0px"),
                                                             options=[''])
        self.botao_add_cc = widgets.Button(description="Inserir CC",
                                               button_style='info',
                                               layout=widgets.Layout(width="120px", height='30px', margin="0px 0px 0px 0px"))
        
        label_cc_removidas_massiva = widgets.Label('Remover Conta da Seleção:', layout=widgets.Layout(width='250px', height='50px', margin="0px 0px 0px 0px"))
        self.cc_removidas_massiva = widgets.Text(layout=widgets.Layout(width="200px", height='30px', margin="0px 40px 0px 0px"),
                                                             value='')
        self.botao_cc_removidas = widgets.Button(description="Remover Conta Contrato",
                                               button_style='danger',
                                               layout=widgets.Layout(width="400px", height='30px', margin="0px 0px 0px 0px"))
        
        label_atualizar_mapa = widgets.Label('Atualizar Mapa da Seleção',layout=widgets.Layout(width='250px', height='50px', margin="0px 0px 0px 0px"))
        self.botao_atualizar_mapa = widgets.Button(description="Atualizar Mapa",
                                               button_style='warning',
                                               layout=widgets.Layout(width="400px", height='30px', margin="0px 0px 0px 0px"))
        
        
        container_zonas_pselecionar = widgets.HBox(children = [label_zonas_pselecionar,
                                                              self.drop_grupos_massiva,
                                                              self.zonas_pselecionar,
                                                              self.botao_zonas_pselecionar])
        container_add_cliente_cc_sm = widgets.HBox(children = [label_add_cliente_cc_sm,
                                                              self.add_cliente_cc_sm_text,
                                                              label_add_cliente_clusterid,
                                                              self.add_cliente_clusterid_sm, 
                                                              self.botao_add_cc])  
        
        container_cc_removidas_massiva= widgets.HBox(children = [label_cc_removidas_massiva,
                                                              self.cc_removidas_massiva,
                                                              self.botao_cc_removidas]) 
        container_atualizar_mapa = widgets.HBox(children = [label_atualizar_mapa,
                                                           self.botao_atualizar_mapa])
        self.botao_zonas_pselecionar.on_click(self.inserir_zonas)
        self.botao_add_cc.on_click(self.inserir_conta_massiva)
        self.botao_cc_removidas.on_click(self.remover_conta_massiva)  
        self.botao_atualizar_mapa.on_click(self.atualizar_mapa)
        
        
        selecao_massiva_box =  widgets.VBox(children=[self.status_parametros_sm,
                                                      container_parametros_sm,
                                                      container_nome_arquivo,
                                                      container_parametros_destaque_sm,
                                                      container_zonas_pselecionar,
                                                      container_add_cliente_cc_sm,
                                                      container_cc_removidas_massiva,
                                                      container_atualizar_mapa,
                                                      botao_realizar_sm,
                                                      container_links_arquivos,
                                                      self.html_tabela_resultados_sm,
                                                      self.html_selecao_massiva])
        
        return selecao_massiva_box
    

   
    def resultados_hana(self):
        #Salva um Resultado em uma Table no HANA
        #Labels e Widgets
        self.status_consulta_hana= widgets.HTML("""""")
        self.resultado_conf= widgets.HTML("""""")
        self.resultado_conf_csv = widgets.HTML("""""")
        
        self.arquivo_hana = widgets.Dropdown(layout=widgets.Layout(width="310px", height='30px', margin="0px 20px 0px 0px")                                                                                           ,options=self.resultados_massiva)                                                       
        label_nome_hana = widgets.Label('Arquivo Para Inserção na Tabela:',layout=widgets.Layout(width='300x', height='45px', margin="0px 20px 0px 0px"))
        
        
        
        self.flag_def = widgets.Dropdown(layout=widgets.Layout(width="116px", height='30px', margin="0px 40px 0px 0px"),
                                                             options=['N','S']
                                                             )
        label_flag_def = widgets.Label('Flag Defasagem:',layout=widgets.Layout(width='150x', height='45px', margin="0px 20px 0px 0px"))
        
        self.botao_inserir_cluster = widgets.Button(description="Inserir na Tabela",
                                               button_style='success',
                                               layout=widgets.Layout(width="150px", height='30px', margin="0px 20px 0px 0px"))
        
        
        
        self.botao_inserir_cluster.on_click(self.cluster_hist)
        #self.botao_atualizar_resultados.on_click(self.att_consulta_cluster_hist)
        
        

        arquivo_hana_box= widgets.HBox(children=[label_nome_hana,                                                           self.arquivo_hana,self.botao_inserir_cluster])
        
        #Labels e Widgets
        
        label_carteira_wgt = widgets.Label('Carteira (Configuração e Inserção na Tabela):',layout=widgets.Layout(width='400x', height='45px', margin="0px 20px 0px 0px"))
        self.carteira_wgt = widgets.Dropdown(value= self.carteira_sm,layout=widgets.Layout(width="150px", height='30px', margin="0px 20px 0px 0px"),
                                                             options= self.opcoes_conf_sm )
         #Primeira HBox
        flag_def_box = widgets.HBox(children=[label_carteira_wgt, self.carteira_wgt,
                                             label_flag_def, self.flag_def])
        
        
        
        label_peso_mtv_cob_wgt = widgets.Label('Peso MTVCOB:',layout=widgets.Layout(width='100px', height='45px', margin="0px 15px 0px 0px"))
        self.peso_mtv_cob_wgt =  widgets.FloatText(value=self.peso_mtv_cob_wgt_value,layout=widgets.Layout(width="50px", height='30px', margin="0px 15px 0px 0px"))
                                                   
        label_peso_pecld_wgt = widgets.Label('Peso PECLD:',layout=widgets.Layout(width='100px', height='45px', margin="0px 15px 0px 0px"))
        self.peso_pecld_wgt =  widgets.FloatText(value=self.peso_pecld_wgt_value,layout=widgets.Layout(width="50px", height='30px', margin="0px 15px 0px 0px")
                                                             )
        label_peso_qtdftve_wgt = widgets.Label('Peso QTDFTVE:',layout=widgets.Layout(width='100px', height='45px', margin="0px 15px 0px 0px"))
        self.peso_qtdftve_wgt =  widgets.FloatText(value=self.peso_qtdftve_wgt_value,layout=widgets.Layout(width="50px", height='30px', margin="0px 15px 0px 0px"))
        
        self.botao_importar_conf= widgets.Button(description = "Importar Config", button_style ='info', layout=widgets.Layout(width="150px", height='30px', margin="0px 15px 0px 0px"))
         
        self.botao_importar_conf.on_click (self.importar_clusterconf)                                          
        #Segunda HBox
                    
        label_percent_ram = widgets.Label('Ram em Utilização:',layout = widgets.Layout(width = '150px',height = '45px',margin = "20px 10px 0px 0px"))
        self.percent_ram = widgets.Text(value = str(virtual_memory()[2])+'%',layout = widgets.Layout(width = '70px',height = '45px',margin = "20px 20px 0px 0px"), disabled = True)
        
        label_cpu_percent = widgets.Label('CPU em Utilização:',layout = widgets.Layout(width = '150px',height = '45px',margin = "20px 10px 0px 0px"))
        self.percent_cpu = widgets.Text(value = str(cpu_percent())+'%',layout = widgets.Layout(width = '70px',height = '45px',margin = "20px 20px 0px 0px"), disabled = True)
         
        percent_hbox = widgets.HBox(children = [label_percent_ram,self.percent_ram,
                                               label_cpu_percent, self.percent_cpu])
            
        conf_import_box = widgets.HBox(children = [
                                              label_peso_mtv_cob_wgt,self.peso_mtv_cob_wgt,
                                              label_peso_pecld_wgt,self.peso_pecld_wgt,
                                              label_peso_qtdftve_wgt,self.peso_qtdftve_wgt,
                                              self.botao_importar_conf,
                                               ])                                         
        
        #VBox
                                                   
        resultados_hana = widgets.VBox(children=[self.status_consulta_hana,
                                                 self.resultado_conf,
                                                 self.resultado_conf_csv,
                                                 flag_def_box,arquivo_hana_box,
                                                 conf_import_box,
                                                 percent_hbox])
        
        return resultados_hana
        
        
        

    #  -------------- Fim: Tabs da interface -----------------
    def show(self):
        try:
            parametros_box = widgets.VBox([widgets.HTML(self.style),
                                           self.parametros_selecao()])
            testagem_r_box = widgets.VBox([self.testagem_r()], layout=widgets.Layout(min_height='500px'))
            selecao_box = widgets.VBox([self.selecionar()])
            resultados_selecao_box = widgets.VBox([self.selecao_resultados()], layout=widgets.Layout(min_height='500px'))
            maiores_devedores_box = self.maiores_devedores()
            selecao_massiva_box = self.selecao_massiva() 
            resultados_hana_box = self.resultados_hana()
            
            thread_percent = threading.Thread(target = self.utilization_percent)
            thread_percent.start()
            
            tab = widgets.Tab([parametros_box, 
                               testagem_r_box, 
                               selecao_box, 
                               resultados_selecao_box, 
                               maiores_devedores_box,
                               selecao_massiva_box,
                               resultados_hana_box])
            tab.set_title(index=0, title="Parâmetros")
            tab.set_title(index=1, title="Testagem de raios")
            tab.set_title(index=2, title="Selecionar")
            tab.set_title(index=3, title="Resultados")
            tab.set_title(index=4, title="Maiores devedores")
            tab.set_title(index=5, title="Seleção Massiva")
            tab.set_title(index=6, title="HANA")

            return tab
        except Exception as e:
            print("Não foi possível carregar a interface", e)
