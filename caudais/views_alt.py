from django.shortcuts import render
from django.http import HttpResponse
from django.http import JsonResponse
from .models import Regiao, PontoMedida, Serie, Medicao, MedicaoProcessada,EstatisticaMensal,EstatisticaAnual
from .forms import *
from.funcoes import carregar_excel,guardaProcessados,guardaEstatisticaAnual,guardaEstatisticaMensal
from django.db.models.functions import ExtractYear, ExtractMonth
from django.db.models import Sum, Count, Avg
import pandas as pd
import calendar
import math
from .funcoes import normalize
from rpy2.robjects import pandas2ri
import rpy2.robjects as robjects
from rpy2.robjects.conversion import localconverter
from rpy2.robjects import default_converter
from rpy2.robjects import conversion
from django.contrib.auth.decorators import login_required
from statistics import quantiles, median
from django.db.models import Min, Max, Avg
import json
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import base64
import numpy as np
conversion.set_conversion(default_converter + pandas2ri.converter)
R_SCRIPT_PATH = 'C:\\Users\\nicol\\Documents\\a22203874-projeto-pw\\caudais\\r_scripts\\reconstruction_script.R'
@login_required(login_url='/autenticacao/login/')
def upload_novo_ponto(request):
    if request.method == 'POST':
        selection_form = UploadSelectionForm(request.POST)
        serieName_form = SerieNovaComPontoNovoForm(request.POST)
        regiao_form = RegiaoForm(request.POST)
        ponto_form = PontoMedidaForm(request.POST)
        arquivo_form = ArquivoExcelForm(request.POST, request.FILES)

       
        forms_validos = all([
            selection_form.is_valid(),
            serieName_form.is_valid(),
            regiao_form.is_valid(),
            ponto_form.is_valid(),
            arquivo_form.is_valid()
        ])

        if forms_validos:
            nome_serie = serieName_form.cleaned_data.get('nome_serie')

           
            regiao,_= Regiao.objects.get_or_create(
                nome=regiao_form.cleaned_data['regiao_nome'],
                localidade=regiao_form.cleaned_data['regiao_localidade']
            )
           
            ponto = PontoMedida.objects.create(
                    user=request.user,
                    regiao=regiao,
                    tipoMedidor=ponto_form.cleaned_data['tipo_medidor'],
                    latitude=ponto_form.cleaned_data['latitude'],
                    longitude=ponto_form.cleaned_data['longitude']
                )
            serie = Serie.objects.create(ponto_medida=ponto, nome=nome_serie)
            mensagem = carregar_excel(arquivo_form.cleaned_data['arquivo_excel'], serie)
            return render(request, 'caudais/upload_success.html', {'message': mensagem})

    else:
        selection_form = UploadSelectionForm(initial={"modo": "novo"}) 
        serieName_form = SerieNovaComPontoNovoForm()
        regiao_form = RegiaoForm()
        ponto_form = PontoMedidaForm()
        arquivo_form = ArquivoExcelForm()

    return render(request, 'caudais/upload_novo_ponto_serie.html', {
        'selection_form': selection_form,
        'serieName_form': serieName_form,
        'regiao_form': regiao_form,
        'ponto_form': ponto_form,
        'arquivo_form': arquivo_form
    })

@login_required(login_url='/autenticacao/login/')
def upload_nova_serie(request):
    if request.method == 'POST':
        selection_form= UploadSelectionForm(request.POST)
        novaSerie_form= NovaSerieNoPontoExistenteForm(request.POST, user=request.user)
        arquivo_form = ArquivoExcelForm(request.POST, request.FILES)

        if novaSerie_form.is_valid() and arquivo_form.is_valid():
            nome_serie = novaSerie_form.cleaned_data.get('nome_serie')
            ponto_medida = novaSerie_form.cleaned_data.get('ponto_medida')
            serie = Serie.objects.create(ponto_medida=ponto_medida, nome=nome_serie)
            mensagem = carregar_excel(arquivo_form.cleaned_data['arquivo_excel'], serie)
            return render(request, 'caudais/upload_success.html', {'message': mensagem})

    else:
        selection_form = UploadSelectionForm(initial={"modo": "associarSerie"})
        novaSerie_form=NovaSerieNoPontoExistenteForm(user=request.user)
        arquivo_form = ArquivoExcelForm()

    return render(request, 'caudais/upload_nova_serie_existente.html', {
        'selection_form':selection_form,
        'novaSerie_form':  novaSerie_form,
        'arquivo_form': arquivo_form
    })

@login_required(login_url='/autenticacao/login/')
def upload_adicionar_valores(request):
    if request.method == 'POST':
        selection_form= UploadSelectionForm(request.POST)
        adicionar_Valores_serie_form= AdicionarValoresSerieExistenteForm(request.POST, user=request.user)
        arquivo_form = ArquivoExcelForm(request.POST, request.FILES)

        if adicionar_Valores_serie_form.is_valid() and arquivo_form.is_valid():
            serie = adicionar_Valores_serie_form.cleaned_data.get('serie_existente')
            if serie:

                MedicaoProcessada.objects.filter(serie=serie).delete()
                EstatisticaAnual.objects.filter(serie=serie).delete()
                EstatisticaMensal.objects.filter(serie=serie).delete()
                mensagem = carregar_excel(arquivo_form.cleaned_data['arquivo_excel'], serie)
                return render(request, 'caudais/upload_success.html', {'message': mensagem})

    else:
        selection_form = UploadSelectionForm(initial={"modo": "adicionar_valores"})
        adicionar_Valores_serie_form= AdicionarValoresSerieExistenteForm(user=request.user)
        arquivo_form = ArquivoExcelForm()

    return render(request, 'caudais/upload_adicionar_valores.html', {
        'selection_form':selection_form,
        'adicionar_Valores_serie_form':adicionar_Valores_serie_form,
        'arquivo_form': arquivo_form
    })




def obter_series_por_ponto(request):
    ponto_id = request.GET.get('ponto_id')
    series = Serie.objects.filter(ponto_medida_id=ponto_id).values('id', 'nome')
    return JsonResponse(list(series), safe=False)



def get_anos_por_serie(request):
    serie_id = request.GET.get("serie_id")
    if not serie_id:
        return JsonResponse({'anos': []})
    
    anos = (
        Medicao.objects.filter(serie_id=serie_id)
        .annotate(ano=ExtractYear("timestamp"))
        .values_list("ano", flat=True)
        .distinct()
    )
    return JsonResponse({'anos': sorted(anos)})

def calcula_outliers(serie, metodo, ano, mes, q1, q3):
    if metodo == 'raw':
        valores = Medicao.objects.filter(
            serie=serie, timestamp__year=ano, timestamp__month=mes
        ).values_list('valor', flat=True)
    else:
        valores = MedicaoProcessada.objects.filter(
            serie=serie, metodo=metodo, timestamp__year=ano, timestamp__month=mes
        ).values_list('valor', flat=True)

    # Remover valores None antes de análise
    valores_np = np.array([v for v in valores if v is not None])
    
    if len(valores_np) == 0:
        return []

    iqr = q3 - q1
    return valores_np[(valores_np < q1 - 1.5 * iqr) | (valores_np > q3 + 1.5 * iqr)].tolist()

def calculate_boxplot_data(querySet, selected_serie, metodo, selected_year, calcular):
    monthly_stats = {}

    if not calcular:
        estatisticas_mensais = EstatisticaMensal.objects.filter(
            serie=selected_serie,
            ano=selected_year,
            metodo=metodo  
        ).order_by('mes')

        for est in estatisticas_mensais:
            outliers = calcula_outliers(
                selected_serie, metodo, selected_year, est.mes, est.q1, est.q3
            )

            monthly_stats[est.mes] = {
                "min": est.minWhisker,
                "q1": est.q1,
                "median": est.medianaMensal,
                "mean": est.media,
                "q3": est.q3,
                "max": est.maxWhisker,
                "outliers": outliers
            }

    else:
        df = pd.DataFrame(list(querySet.values("timestamp", "valor")))

        if df.empty:
            return {}

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["mes_num"] = df["timestamp"].dt.month
        df["mes_nome"] = df["timestamp"].dt.strftime("%b")
        df = df.dropna(subset=["valor"])

        meses_com_dados = df.groupby("mes_num").filter(lambda x: len(x) >= 5)
        if meses_com_dados.empty:
            return {}

        for month_num in sorted(meses_com_dados["mes_num"].unique()):
            month_data = meses_com_dados[meses_com_dados["mes_num"] == month_num]["valor"]

            if len(month_data) >= 5:
                q1 = month_data.quantile(0.25)
                median = month_data.median()
                q3 = month_data.quantile(0.75)
                mean = month_data.mean()
                iqr = q3 - q1
                lower_whisker = max(month_data.min(), q1 - 1.5 * iqr)
                upper_whisker = min(month_data.max(), q3 + 1.5 * iqr)

                outliers = month_data[(month_data < lower_whisker) | (month_data > upper_whisker)].tolist()

                monthly_stats[int(month_num)] = {
                    'min': float(lower_whisker),
                    'q1': float(q1),
                    'median': float(median),
                    'mean': float(mean),
                    'q3': float(q3),
                    'max': float(upper_whisker),
                    'outliers': [float(x) for x in outliers]
                }

    return dict(sorted(monthly_stats.items()))


def dadosGraficoTodosInstantes(dados_serie):
    result = {
        "labels": [],
        "valores": []
    }
    valores = []

    df = pd.DataFrame(list(dados_serie.values("timestamp", "valor")))

    if not df.empty:
        
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
            
        for val in df["valor"]:
            if pd.isna(val):               # detecta NaN ou None:contentReference[oaicite:3]{index=3}
                valores.append(None)       # mantém nulo para representar gap no gráfico
            else:
                valores.append(val) 

        result["labels"] = [ts.strftime("%Y-%m-%d %H:%M:%S") for ts in df["timestamp"]]
        result["valores"] = [v for v in valores]

    return result

def dadosGraficoLinhas(dados_diarios):
    result = {
        "labels": [],
        "valores": []
    }

    df = pd.DataFrame(list(dados_diarios.values("timestamp", "valor")))

    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["data"] = df["timestamp"].dt.date
        media_por_dia = df.groupby("data")["valor"].mean()

        # Intervalo completo de datas
        full_range = pd.date_range(start=df["data"].min(), end=df["data"].max(), freq="D")
        media_completa = media_por_dia.reindex(full_range.date)  # preserva datas ausentes com NaN

        # Labels e valores com None onde não há dados
        linha_temporal_labels = [d.strftime("%Y-%m-%d") for d in full_range]
        linha_temporal_valores = [round(v, 2) if pd.notnull(v) else None for v in media_completa]
        
       
        result["labels"] = linha_temporal_labels
        result["valores"] = linha_temporal_valores

    return result

@login_required(login_url='/autenticacao/login/')
def dashboard(request):
    conversion.set_conversion(default_converter + pandas2ri.converter)
    # Get query parameters
    selected_year = request.GET.get('year')
    selected_ponto_medicao_id = request.GET.get('ponto_medicao')
    selected_serie_id = request.GET.get('serie_id')
    data_type = request.GET.get('data_type', 'raw')  # default para raw 
    recon_method = request.GET.get('recon_method', 'jq') 

    # buscar todos PontoMedida para dropdown DO USER
    pontos_medicao =  PontoMedida.objects.filter(user=request.user)
    
    ##### NOTAS ####
    ##Ainda falta melhorar o zoom do grafico de linhas completo(a serie toda)
    ##Queria meter um miniMap para o zoom maybe,
    ##ou selecao do intrevalo e aquilo fazer zoom para esses intrevalo 
    ##porque o zoom nao esta muito nice e e dificil de analisar as linhas 
    ##sem os Zooms, eles normalmente querem ver a diferenca se os dados
    ##forma realmente recontruidos(eles comparam os normalizados e depois os 
    #reconstruidos)
    #E falta melhorar A rapidez, esta um pouco lento mesmo depois de guardar os dados
    #por causa dos queries e nao consegui guardar os outliers dos boxplots,
    #por isso volto a calcular
    #mudei o recontructedScript-Correcoes na funcao do Leandro
    #Nao testei o grafico de linhas completo para o recontrucao tbats e jq


###Necessario Para A troca entre series do mesmo ponto funcionar bem###
###################################################################################################################

 ###Vejam o javascript do dahsboard
 ##tenho funcoes que automaticamente buscam as series de um ponto 
 ## e que vao buscar tb automaticamente anos de uma serie
    
    if selected_ponto_medicao_id:
        selected_ponto_medicao = PontoMedida.objects.get(id=selected_ponto_medicao_id)
    else:
        selected_ponto_medicao = None
    
    series = Serie.objects.filter(ponto_medida=selected_ponto_medicao) if selected_ponto_medicao else []
    
    if selected_serie_id :
        selected_serie = Serie.objects.get(id=selected_serie_id )
    else:
        selected_serie = None


    if selected_year:
        try:
            selected_year = int(selected_year)
        except ValueError:
            selected_year = None
    else:
            selected_year = None
    

    if selected_serie:
        anos_disponiveis = Medicao.objects.filter(serie=selected_serie).annotate(
            ano=ExtractYear('timestamp')
        ).values_list('ano', flat=True).distinct()

        if selected_year and selected_year not in anos_disponiveis:
            selected_year = min(anos_disponiveis) if anos_disponiveis else None
        

 ######################################################################################   

    # Initialize empty variables for charts
    years, counts, totals, avg_values = [], [], [], []
    month_labels, month_counts, month_totals, month_avg = [], [], [], []
 
    boxplot_data = {}
    dados_gragico_linhas={}
    dados_gragico_linhas_instantesT={}
    labels_grafico_linhasT=[]
    valores_grafico_linhasT=[]
    labels_grafico_linhas=[]
    valores_grafico_linhas=[]

    # Branch for data_type == 'raw'
    if data_type == 'raw':
        serie_dataT = Medicao.objects.filter(serie=selected_serie).order_by('timestamp')
        dados_gragico_linhas_instantesT=dadosGraficoTodosInstantes(serie_dataT)
       
        # Tenta carregar estatísticas do banco
        estatisticas_anuais = EstatisticaAnual.objects.filter(
        serie=selected_serie,
        metodo=data_type
        )

        if estatisticas_anuais.exists():
            

            for e in estatisticas_anuais:
                years.append(e.ano)
                totals.append(e.total)
                counts.append(e.contagem)
                avg_values.append(e.media)

        else:       

            
            # Query yearly data from raw Medicao records
            yearly_data = Medicao.objects.filter(serie=selected_serie).annotate(
            year=ExtractYear('timestamp')
            ).values('year').annotate(
            total_valor=Sum('valor'), count=Count('id'), avg_valor=Avg('valor')
            ).order_by('year')

            years = [entry['year'] for entry in yearly_data]
            counts = [entry['count'] for entry in yearly_data]
            totals = [entry['total_valor'] for entry in yearly_data]
            avg_values = [entry['avg_valor'] for entry in yearly_data]

            
            
            guardaEstatisticaAnual(zip(years, totals, counts, avg_values),data_type,selected_serie)
        # Set default year if none is provided
        if selected_year:
            try:
                selected_year = int(selected_year)
            except ValueError:
                selected_year = None
        else:
            selected_year = years[-1] if years else None

        # Tenta carregar estatísticas do banco
        estatisticas_mensais= EstatisticaMensal.objects.filter(
        serie=selected_serie,
        ano=selected_year,
        metodo=data_type

        )
        serie_data = Medicao.objects.filter(serie=selected_serie, timestamp__year=selected_year)
        if estatisticas_mensais.exists():
           
            boxplot_data = calculate_boxplot_data(serie_data,selected_serie,data_type,selected_year,False)
            for e in estatisticas_mensais:
                month_labels.append(e.mes)
                month_totals.append(e.total)
                month_counts.append(e.contagem)
                month_avg.append(e.media)  


        else:
            # Query monthly raw data for the selected year
            if selected_year:
                monthly_data = Medicao.objects.filter(
               serie=selected_serie, timestamp__year=selected_year
                ).annotate(
                month=ExtractMonth('timestamp')
                ).values('month').annotate(
                total_valor=Sum('valor'), count=Count('id'), avg_valor=Avg('valor')
                ).order_by('month')

                monthly_lookup = {entry['month']: entry for entry in monthly_data}
                for m in range(1, 13):
                    month_labels.append(m)
                    if m in monthly_lookup:
                        entry = monthly_lookup[m]
                        month_counts.append(entry['count'])
                        month_totals.append(entry['total_valor'])
                        month_avg.append(entry['avg_valor'])
                    else:
                        month_counts.append(0)
                        month_totals.append(0)
                        month_avg.append(0)
                
                boxplot_data = calculate_boxplot_data(serie_data,selected_serie,data_type,selected_year,True)

                dados_completos = []

                for i, mes in enumerate(month_labels):
                    if mes in boxplot_data:
                        dados_completos.append((
                        mes,
                        month_totals[i] if month_totals[i] is not None else 0.0,
                        month_counts[i] if month_counts[i] is not None else 0,
                        month_avg[i] if month_avg[i] is not None else 0.0,
                        boxplot_data[mes].get('min', 0.0),
                        boxplot_data[mes].get('max', 0.0),
                        boxplot_data[mes].get('median', 0.0),
                        boxplot_data[mes].get('q1', 0.0),
                        boxplot_data[mes].get('q3', 0.0),
                        ))
                    else:
                        dados_completos.append((
                        mes,
                        month_totals[i],
                        month_counts[i],
                        month_avg[i],
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        ))
                        


                guardaEstatisticaMensal(dados_completos,data_type,selected_serie,selected_year)
        
    # Branch for data_type == 'normalized'
    if data_type == 'normalized':
        # Query the raw Medicao data for the selected PontoMedicao ,serie and year
        dadosRaw = Medicao.objects.filter(serie=selected_serie)
        df = pd.DataFrame(list(dadosRaw.values('timestamp', 'valor')))
        dados_guardados = MedicaoProcessada.objects.filter(
        serie=selected_serie,
        metodo=recon_method if data_type == 'reconstruido' else 'normalized'
         ).order_by('timestamp')

        #a dados so do ano selecionado
        serie_data = MedicaoProcessada.objects.filter(
        serie=selected_serie,
        metodo='normalized',timestamp__year=selected_year).order_by('timestamp')
        #grafico de linhas diario
        dados_gragico_linhas=dadosGraficoLinhas(serie_data)
        labels_grafico_linhas=dados_gragico_linhas["labels"]
        valores_grafico_linhas=dados_gragico_linhas["valores"]
        
        #a serie toda todos os instantes
        serie_dataT = MedicaoProcessada.objects.filter(
        serie=selected_serie,
        metodo='normalized').order_by('timestamp')
        #grafico de linhas Total, ou seja completo todos os instantes
        dados_gragico_linhas_instantesT=dadosGraficoTodosInstantes(serie_dataT)
        labels_grafico_linhasT=dados_gragico_linhas_instantesT["labels"]
        valores_grafico_linhasT=dados_gragico_linhas_instantesT["valores"]

        if dados_guardados.exists():
            df = pd.DataFrame(list(dados_guardados.values('timestamp', 'valor')))
            df.set_index('timestamp', inplace=True)
            resampled_df = df 
        else:
            if not df.empty:
                # Convert to DataFrame and normalize
                df['timestamp'] = pd.to_datetime(df['timestamp'])#transforma o timestamp em datetime do pandas
                df.set_index('timestamp', inplace=True)
                df.index = df.index.tz_localize(None)
                #tranforma a serie cuma serie continuam de intrevalos fixos 15 minutos,se nao existir valor em alguns dos intrevalos e colocado NaN
                resampled_df = df.resample('15T').asfreq()
                year_end = df.index.max().year
                month_end = df.index.max().month
                last_day = calendar.monthrange(year_end, month_end)[1] # Use calendar.monthrange to get the last day of the month

                start_date = pd.Timestamp(f"{df.index.min().year}-{df.index.min().month}-01") 
                end_date = pd.Timestamp(f"{year_end}-{month_end}-{last_day} 23:45:00") # End at the last day of the final year
                # Create a new date range with 15-minute intervals for the entire period
                full_range = pd.date_range(start=start_date, end=end_date, freq='15T')
                #Resample and apply frequency (asfreq will generate NaNs for missing intervals)
                resampled_df = df.resample('15T').asfreq()
                #Reindex the DataFrame to include the full date range, filling missing periods with NaN
                resampled_df = resampled_df.reindex(full_range)
                normalize(df,resampled_df, 15)  # aplicacao de funcao de normalizacao dos leandro
                guardaProcessados(resampled_df['valor'].items(),'normalized',selected_serie)
            ## Query yearly data dos dados normalizados

        # Tenta carregar estatísticas do banco
        estatisticas_anuais = EstatisticaAnual.objects.filter(
        serie=selected_serie,
        metodo=data_type
        )

        if estatisticas_anuais.exists():
            for e in estatisticas_anuais:
                years.append(e.ano)
                totals.append(e.total)
                counts.append(e.contagem)
                avg_values.append(e.media)
                
        else:

            yearly_normalized = resampled_df.groupby(resampled_df.index.year).agg(
            total_valor=('valor', 'sum'),
            count=('valor', 'count'),
            avg_valor=('valor', 'mean'))

            years = yearly_normalized.index.tolist()
            totals = yearly_normalized['total_valor'].tolist()
            counts = yearly_normalized['count'].tolist()
            avg_values = [float(x) for x in yearly_normalized['avg_valor'].tolist()]
            
            guardaEstatisticaAnual(zip(years, totals, counts, avg_values),data_type,selected_serie)

        if selected_year:
            try:
                selected_year = int(selected_year)
            except ValueError:
                selected_year = None
        else:
            selected_year = years[-1] if years else None

        # Tenta carregar estatísticas do banco
        estatisticas_mensais= EstatisticaMensal.objects.filter(
        serie=selected_serie,
        ano=selected_year,
        metodo=data_type

        )
        if estatisticas_mensais.exists():
            boxplot_data = calculate_boxplot_data(serie_data,selected_serie,data_type,selected_year,False)
            for e in estatisticas_mensais:
                month_labels.append(e.mes)
                month_totals.append(e.total)
                month_counts.append(e.contagem)
                month_avg.append(e.media)
        else:
            if selected_year:
                # Recalculate monthly statistics from normalized data
                resampled_df_selected_year = resampled_df[resampled_df.index.year == selected_year]
                monthly_normalized = resampled_df_selected_year.groupby(resampled_df_selected_year.index.month).agg(
                    count=('valor', 'count'),
                    total_valor=('valor', 'sum'),
                    avg_valor=('valor', 'mean')
                ).reindex(range(1, 13), fill_value=0)

                # Assign monthly values for charts
                # Assign monthly values for charts
                month_counts = [int(x) if pd.notnull(x) and not math.isnan(x) else 0
                               for x in monthly_normalized['count'].tolist()
                               ]

                month_totals = [float(x) if pd.notnull(x) and not math.isnan(x) else 0.0
                                for x in monthly_normalized['total_valor'].tolist()
                                ]

                month_avg = [float(x)  if pd.notnull(x) and not math.isnan(x) else 0.0
                             for x in monthly_normalized['avg_valor'].tolist()
                            ]

                month_labels = [i for i in range(1, 13)]
                boxplot_data = calculate_boxplot_data(serie_data,selected_serie,data_type,selected_year,True)


                dados_completos = []

                for i, mes in enumerate(month_labels):
                    if mes in boxplot_data:
                        dados_completos.append((
                        mes,
                        month_totals[i],
                        month_counts[i],
                        month_avg[i],
                        boxplot_data[mes].get('min', 0.0),
                        boxplot_data[mes].get('max', 0.0),
                        boxplot_data[mes].get('median', 0.0),
                        boxplot_data[mes].get('q1', 0.0),
                        boxplot_data[mes].get('q3', 0.0),
                        ))
                    else:
                        dados_completos.append((
                        mes,
                        month_totals[i],
                        month_counts[i],
                        month_avg[i],
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        ))


                guardaEstatisticaMensal(dados_completos,data_type,selected_serie,selected_year)
         

    # Branch for data_type == 'reconstruido' (use the Tbats function)
    if data_type == 'reconstruido':
        # Load your R script
        with localconverter(default_converter + pandas2ri.converter):
            robjects.r.source(R_SCRIPT_PATH)
        
        # Query the raw Medicao data for the selected serie
        dados_raw = Medicao.objects.filter(serie=selected_serie)
        df = pd.DataFrame(list(dados_raw.values('timestamp', 'valor')))

        dados_guardados = MedicaoProcessada.objects.filter(serie=selected_serie,
        metodo=recon_method).order_by('timestamp')

        #dados so do ano selecionado
        serie_data = MedicaoProcessada.objects.filter(
        serie=selected_serie,
        metodo=recon_method,timestamp__year=selected_year).order_by('timestamp')
        #grafico de linhas diario
        dados_gragico_linhas=dadosGraficoLinhas(serie_data)
        labels_grafico_linhas=dados_gragico_linhas["labels"]
        valores_grafico_linhas=dados_gragico_linhas["valores"]

        #serie completa, todos os intantes
        serie_dataT = MedicaoProcessada.objects.filter(
        serie=selected_serie,
        metodo=recon_method).order_by('timestamp')
        dados_gragico_linhas_instantesT=dadosGraficoTodosInstantes(serie_dataT)

        #grafico de linhas Total, ou seja completo todos os instantes
        dados_gragico_linhas_instantesT=dadosGraficoTodosInstantes(serie_dataT)
        labels_grafico_linhasT=dados_gragico_linhas_instantesT["labels"]
        valores_grafico_linhasT=dados_gragico_linhas_instantesT["valores"]

        if dados_guardados.exists():
            df = pd.DataFrame(list(dados_guardados.values('timestamp', 'valor')))
            df.set_index('timestamp', inplace=True)
            resampled_df = df

                
        else:
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                df.index = df.index.tz_localize(None)
               # Resample the data to 15-minute intervals (fill NaN for missing intervals)
                resampled_df = df.resample('15T').asfreq()
                year_end = df.index.max().year
                month_end = df.index.max().month
                last_day = calendar.monthrange(year_end, month_end)[1] # Use calendar.monthrange to get the last day of the month

                start_date = pd.Timestamp(f"{df.index.min().year}-{df.index.min().month}-01")  # Start from January 1st of the first year
                end_date = pd.Timestamp(f"{year_end}-{month_end}-{last_day} 23:45:00") # End at the last day of the final year
                #Create a new date range with 15-minute intervals for the entire period
                full_range = pd.date_range(start=start_date, end=end_date, freq='15T')
                #Resample and apply frequency (asfreq will generate NaNs for missing intervals)
                resampled_df = df.resample('15T').asfreq()
                #Reindex the DataFrame to include the full date range, filling missing periods with NaN
                resampled_df = resampled_df.reindex(full_range)
                resampled_df.index.name = 'Date'#Isto porque a matrix tem que ter uma coluna Date por causa do dma
                #original,resampled
                normalize(df,resampled_df, 15)
                # Since 'Data' is now the index, we use the index to create 'Date' and 'Time' columns
                resampled_df['Date'] = resampled_df.index.strftime('%Y/%m/%d')  # Extract date as YYYY/MM/DD
                resampled_df['Time'] = resampled_df.index.strftime('%H:%M')     # Extract time as HH:MM
            
                matrix_df = resampled_df.pivot(index='Date', columns='Time', values='valor')
                # Reset the index to make 'Date' the first column
                matrix_df.reset_index()
            
                matrix_df.columns.name = None  # This removes the 'Time' label from the columns
                matrix_pronta =matrix_df.reset_index()
                #passa a variavel do python matrixpront para o R environment
                with localconverter(default_converter + pandas2ri.converter):
                    robjects.globalenv['matrix_pronta'] = pandas2ri.py2rpy(matrix_pronta)

                reconstructed_values_list=[]
                if recon_method == 'jq':
                    JQ_function = robjects.globalenv['JQ.function']
                    reconstructedValues = JQ_function()
                    reconstructed_values_list = reconstructedValues.tolist()

                else:
                # Call the TBATS function from R to reconstruct the missing values
                    TBATS_function = robjects.globalenv['TBATS.function']
                    reconstructedValues = TBATS_function()
                    reconstructed_values_list = reconstructedValues.tolist()
             
            
                resampled_df['valor']=reconstructed_values_list
                guardaProcessados(resampled_df['valor'].items(),recon_method,selected_serie)
        
         # Tenta carregar estatísticas do banco
        estatisticas_anuais = EstatisticaAnual.objects.filter(
        serie=selected_serie,
        metodo=recon_method
        )

        if estatisticas_anuais.exists():
            for e in estatisticas_anuais:
                years.append(e.ano)
                totals.append(e.total)
                counts.append(e.contagem)
                avg_values.append(e.media)
        else:       
            # Recalculate yearly statistics
            yearly_reconstructed = resampled_df.groupby(resampled_df.index.year).agg(
                total_valor=('valor', 'sum'),
                count=('valor', 'count'),
                avg_valor=('valor', 'mean')
                )

            years = yearly_reconstructed.index.tolist()
            totals = yearly_reconstructed['total_valor'].tolist()
            counts = yearly_reconstructed['count'].tolist()
            avg_values = [float(x) for x in yearly_reconstructed['avg_valor'].tolist()]
            
            guardaEstatisticaAnual(zip(years, totals, counts, avg_values),data_type,selected_serie)
        if selected_year:
            try:
                selected_year = int(selected_year)
            except ValueError:
                selected_year = None
        else:
            selected_year = years[-1] if years else None

        # Tenta carregar estatísticas do banco
        estatisticas_mensais= EstatisticaMensal.objects.filter(
        serie=selected_serie,
        ano=selected_year,
        metodo=recon_method

        )
        if estatisticas_mensais.exists():
            boxplot_data = calculate_boxplot_data(serie_data,selected_serie,data_type,selected_year,False)
            for e in estatisticas_mensais:
                month_labels.append(e.mes)
                month_totals.append(e.total)
                month_counts.append(e.contagem)
                month_avg.append(e.media)
        else:
            # Recalculate monthly statistics for the selected year
            if selected_year:
                resampled_df_selected_year = resampled_df[resampled_df.index.year == selected_year]
                monthly_reconstructed = resampled_df_selected_year.groupby(resampled_df_selected_year.index.month).agg(
                    count=('valor', 'count'),
                    total_valor=('valor', 'sum'),
                    avg_valor=('valor', 'mean')
                    ).reindex(range(1, 13), fill_value=0)

                # Assign monthly values for charts
                month_counts = [int(x) if pd.notnull(x) and not math.isnan(x) else 0
                               for x in monthly_reconstructed['count'].tolist()
                               ]

                month_totals = [float(x) if pd.notnull(x) and not math.isnan(x) else 0.0
                                for x in monthly_reconstructed['total_valor'].tolist()
                                ]

                month_avg = [ float(x) if pd.notnull(x) and not math.isnan(x) else 0.0
                             for x in monthly_reconstructed['avg_valor'].tolist()
                            ]
                month_labels = [i for i in range(1, 13)]
                boxplot_data = calculate_boxplot_data(serie_data,selected_serie,data_type,selected_year,True)


                dados_completos = []

                for i, mes in enumerate(month_labels):
                    if mes in boxplot_data:
                        dados_completos.append((
                        mes,
                        month_totals[i],
                        month_counts[i], 
                        month_avg[i], 
                        boxplot_data[mes].get('min', 0.0),
                        boxplot_data[mes].get('max', 0.0),
                        boxplot_data[mes].get('median', 0.0),
                        boxplot_data[mes].get('q1',0.0),
                        boxplot_data[mes].get('q3', 0.0),
                        ))
                    else:
                        dados_completos.append((
                        mes,
                        month_totals[i],
                        month_counts[i],
                        month_avg[i],
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        ))


                guardaEstatisticaMensal(dados_completos,recon_method,selected_serie,selected_year)


    # Fallback logic if no valid data_type provided
    else:
        # Handle unexpected data_type
        pass
    month_names=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    # Prepare context for charts
    context = {
        'pontos_medicao': pontos_medicao,
        'series':series,
        'selected_ponto_medicao': selected_ponto_medicao,
        'selected_serie':selected_serie,
        'years': years,
        'counts': counts,
        'totals': totals,
        'avg_values': avg_values,
        'selected_year': selected_year,
        'month_counts': month_counts,
        'month_totals': month_totals,
        'month_avg': month_avg,
        'data_type': data_type,  # So template can reflect the selected option
        'month_names': month_names,
        'recon_method': recon_method,
        'boxplot_data':json.dumps(boxplot_data),
        "linha_temporal_labels": json.dumps(labels_grafico_linhas),
        "linha_temporal_valores": json.dumps(valores_grafico_linhas),
        'linha_temporal_labelsT':json.dumps(labels_grafico_linhasT),
        'linha_temporal_valoresT':json.dumps(valores_grafico_linhasT),
        
    }

    return render(request, 'caudais/dashboard.html', context)
@login_required
def exportar_excel(request):
    serie_id = request.GET.get('serie_id')
    data_type = request.GET.get('data_type', 'raw')
    metodo = request.GET.get('recon_method', 'jq')

    if not serie_id:
        return HttpResponse("Série não especificada.", status=400)

    try:
        serie = Serie.objects.get(id=serie_id)
    except Serie.DoesNotExist:
        return HttpResponse("Série inválida.", status=404)

    if data_type == 'raw':
        queryset = Medicao.objects.filter(serie=serie).values('timestamp', 'valor')
    elif data_type == 'normalized':
        queryset = MedicaoProcessada.objects.filter(serie=serie, metodo='normalized').values('timestamp', 'valor')
    elif data_type == 'reconstruido':
        queryset = MedicaoProcessada.objects.filter(serie=serie, metodo=metodo).values('timestamp', 'valor')
    else:
        return HttpResponse("Tipo de dado inválido.", status=400)

    df = pd.DataFrame(list(queryset))
    if df.empty:
        return HttpResponse("Sem dados para exportar.", status=204)

    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
    df = df.sort_values(by='timestamp')
    df.rename(columns={'timestamp': 'Data', 'valor': 'Caudal'}, inplace=True)

    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    if data_type == "reconstruido":
        nome_arquivo = f"medicoes_{data_type}_{metodo}_serie{serie.id}.xlsx"
    else:
        nome_arquivo = f"medicoes_{data_type}_serie{serie.id}.xlsx"

    response['Content-Disposition'] = f'attachment; filename="{nome_arquivo}"'

    with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Medições')

    return response

@login_required
def dashboard_comparison(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Invalid request method'}, status=405)
    
    try:
        ponto_id = request.POST.get('ponto_medicao')
        data_type = request.POST.get('data_type', 'raw')
        recon_method = request.POST.get('recon_method', 'jq')
        selected_series_ids = request.POST.getlist('selected_series')
        
        datasets = []
        all_labels = set()
        
        for serie_id in selected_series_ids:
            try:
                serie = Serie.objects.get(id=serie_id, ponto_medida__user=request.user)
                selected_years = request.POST.getlist(f'years_{serie_id}')
                
                for year in selected_years:
                    year = int(year)
                    
                    # Get data based on data_type
                    if data_type == 'raw':
                        data = Medicao.objects.filter(
                            serie=serie, 
                            timestamp__year=year
                        ).order_by('timestamp').values('timestamp', 'valor')
                    elif data_type == 'normalized':
                        data = MedicaoProcessada.objects.filter(
                            serie=serie, 
                            metodo='normalized',
                            timestamp__year=year
                        ).order_by('timestamp').values('timestamp', 'valor')
                    elif data_type == 'reconstruido':
                        data = MedicaoProcessada.objects.filter(
                            serie=serie, 
                            metodo=recon_method,
                            timestamp__year=year
                        ).order_by('timestamp').values('timestamp', 'valor')
                    
                    # Convert to format suitable for Chart.js
                    chart_data = []
                    for record in data:
                        if record['valor'] is not None:
                            timestamp_str = record['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                            all_labels.add(timestamp_str)
                            chart_data.append({
                                'x': timestamp_str,
                                'y': float(record['valor'])
                            })
                    
                    dataset_label = f"{serie.nome} - {year}"
                    if data_type == 'reconstruido':
                        dataset_label += f" ({recon_method.upper()})"
                    
                    datasets.append({
                        'label': dataset_label,
                        'data': chart_data
                    })
                    
            except Serie.DoesNotExist:
                continue
            except Exception as e:
                print(f"Error processing serie {serie_id}: {e}")
                continue
        
        # Sort labels chronologically
        sorted_labels = sorted(list(all_labels))
        
        return JsonResponse({
            'labels': sorted_labels,
            'datasets': datasets
        })
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)