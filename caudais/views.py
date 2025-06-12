from django.shortcuts import render
from django.http import HttpResponse
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from .models import Regiao, PontoMedida, Serie, Medicao, MedicaoProcessada,EstatisticaMensal,EstatisticaAnual
from .forms import *
from.funcoes import carregar_excel,guardaProcessados,guardaEstatisticaAnual,guardaEstatisticaMensal
from django.db.models.functions import ExtractYear, ExtractMonth
from django.db.models import Sum, Count, Avg
import pandas as pd
import calendar
import math
import json
from .funcoes import normalize
from rpy2.robjects import pandas2ri
import rpy2.robjects as robjects
from rpy2.robjects.conversion import localconverter
from rpy2.robjects import default_converter
from rpy2.robjects import conversion
import os
from django.conf import settings
import numpy as np
from statistics import quantiles, median
from django.db.models import Min, Max, Avg
conversion.set_conversion(default_converter + pandas2ri.converter)
R_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), 'r_scripts', 'reconstruction_script.R')

def calculate_boxplot_data(queryset, selected_serie=None, metodo='raw', selected_year=None, calcular=True):
    monthly_stats = {}

    if not calcular and selected_serie:
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
                "min": float(est.minWhisker),
                "q1": float(est.q1),
                "median": float(est.medianaMensal),
                "mean": float(est.media),
                "q3": float(est.q3),
                "max": float(est.maxWhisker),
                "outliers": [float(x) for x in outliers] if outliers else []
            }

    else:
        df = pd.DataFrame(list(queryset.values("timestamp", "valor")))

        if df.empty:
            return {}

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["mes_num"] = df["timestamp"].dt.month
        df["mes_nome"] = df["timestamp"].dt.strftime("%b")
        df = df.dropna(subset=["valor"])

        if df.empty:
            return {}

        for month_num in range(1, 13):
            month_data = df[df["mes_num"] == month_num]["valor"]

            if len(month_data) >= 1:
                if len(month_data) == 1:
                    value = float(month_data.iloc[0])
                    monthly_stats[int(month_num)] = {
                        'min': value,
                        'q1': value,
                        'median': value,
                        'mean': value,
                        'q3': value,
                        'max': value,
                        'outliers': []
                    }
                elif len(month_data) == 2:
                    values = month_data.sort_values()
                    min_val = float(values.iloc[0])
                    max_val = float(values.iloc[1])
                    mean_val = float(values.mean())
                    monthly_stats[int(month_num)] = {
                        'min': min_val,
                        'q1': min_val,
                        'median': mean_val,
                        'mean': mean_val,
                        'q3': max_val,
                        'max': max_val,
                        'outliers': []
                    }
                else:
                    q1 = float(month_data.quantile(0.25))
                    median = float(month_data.median())
                    q3 = float(month_data.quantile(0.75))
                    mean = float(month_data.mean())
                    iqr = q3 - q1
                    
                    if iqr == 0:
                        monthly_stats[int(month_num)] = {
                            'min': float(month_data.min()),
                            'q1': q1,
                            'median': median,
                            'mean': mean,
                            'q3': q3,
                            'max': float(month_data.max()),
                            'outliers': []
                        }
                    else:
                        lower_whisker = max(float(month_data.min()), q1 - 1.5 * iqr)
                        upper_whisker = min(float(month_data.max()), q3 + 1.5 * iqr)
                        
                        outliers = month_data[(month_data < lower_whisker) | (month_data > upper_whisker)]
                        outliers_list = [float(x) for x in outliers.tolist()]

                        monthly_stats[int(month_num)] = {
                            'min': lower_whisker,
                            'q1': q1,
                            'median': median,
                            'mean': mean,
                            'q3': q3,
                            'max': upper_whisker,
                            'outliers': outliers_list
                        }

    return dict(sorted(monthly_stats.items()))

def calculate_daily_line_data(queryset):
    df = pd.DataFrame(list(queryset.values("timestamp", "valor")))
    
    result = {
        "labels": [],
        "values": []
    }
    
    if df.empty:
        return result
    
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["data"] = df["timestamp"].dt.date
    media_por_dia = df.groupby("data")["valor"].mean()

    
    full_range = pd.date_range(start=df["data"].min(), end=df["data"].max(), freq="D")
    media_completa = media_por_dia.reindex(full_range.date)

    
    linha_temporal_labels = [d.strftime("%Y-%m-%d") for d in full_range]
    linha_temporal_valores = [round(float(v), 2) if pd.notnull(v) else None for v in media_completa]
    
    result["labels"] = linha_temporal_labels
    result["values"] = linha_temporal_valores

    return result

def dadosGraficoTodosInstantesantigo(dados_serie):
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
            if pd.isna(val):               # detecta NaN ou None
                valores.append(None)       # mantém nulo para representar gap no gráfico
            else:
                valores.append(val) 

        result["labels"] = [ts.strftime("%Y-%m-%d %H:%M:%S") for ts in df["timestamp"]]
        result["valores"] = [v for v in valores]

    return result

def dadosGraficoTodosInstantes2(dados_serie):
    result = {
        "labels": [],
        "valores": []
    }

    
    df_copy=df = pd.DataFrame(list(dados_serie.values("timestamp", "valor")))
    if not df.empty:
        # Converter timestamp e arredondar para 15 minutos
        df_copy["timestamp"] = pd.to_datetime(df["timestamp"])

        df_copy["timestamp"] = df["timestamp"].dt.floor("15min")  # Alinha os dados ao intervalo desejado
        df_copy.set_index("timestamp", inplace=True)

        # Criar o intervalo completo de 15 em 15 minutos
        start_date = df_copy.index.min().floor("D")
        end_date = df_copy.index.max().ceil("D") + pd.Timedelta(hours=23, minutes=45)
        full_range = pd.date_range(start=start_date, end=end_date, freq="15T")

        # Reindexar para garantir que todos os instantes estão presentes (mesmo os sem dados)
        df_reindexed = df_copy.reindex(full_range)

        result["labels"] = [ts.strftime("%Y-%m-%d %H:%M:%S") for ts in full_range]
        result["valores"] = [
            v if pd.notnull(v) else None for v in df_reindexed["valor"]
        ]

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





@login_required(login_url='/autenticacao/login/')
def dashboard(request):
    conversion.set_conversion(default_converter + pandas2ri.converter) 
    selected_year = request.GET.get('year')
    selected_ponto_medicao_id = request.GET.get('ponto_medicao')
    selected_serie_ids = request.GET.getlist('serie_ids')
    selected_serie_id = request.GET.get('serie_id')
    data_type = request.GET.get('data_type', 'raw')
    recon_method = request.GET.get('recon_method', 'jq') 
    comparison_mode = request.GET.get('comparison_mode', 'false') == 'true'

    
    series_years = {}
    for param_name, param_values in request.GET.lists():
        if param_name.startswith('years_') and param_values:
            try:
                serie_id_from_param = param_name.replace('years_', '')
                selected_years = [int(year) for year in param_values if year]
                if selected_years:
                    series_years[serie_id_from_param] = selected_years
            except ValueError:
                pass

    if selected_serie_id and not selected_serie_ids:
        selected_serie_ids = [selected_serie_id]

    
    if comparison_mode and len(series_years) > 0:
        all_unique_years = set()
        for serie_id, years_list in series_years.items():
            for year in years_list:
                all_unique_years.add(year)
        
        if len(all_unique_years) > 3:
            
            allowed_years = sorted(list(all_unique_years))[:3]
            
            filtered_series_years = {}
            for serie_id, years_list in series_years.items():
                filtered_years = [year for year in years_list if year in allowed_years]
                if filtered_years:
                    filtered_series_years[serie_id] = filtered_years
            series_years = filtered_series_years

    pontos_medicao = PontoMedida.objects.filter(user=request.user)
    series = Serie.objects.all()

    
    if selected_ponto_medicao_id:
        selected_ponto_medicao = PontoMedida.objects.get(id=selected_ponto_medicao_id)
        series_for_point = Serie.objects.filter(ponto_medida=selected_ponto_medicao)
    else:
        selected_ponto_medicao = None
        series_for_point = Serie.objects.none()

    
    selected_series = []
    if selected_serie_ids:
        selected_series = [Serie.objects.get(id=serie_id) for serie_id in selected_serie_ids if serie_id]

    
    series_all_years = {}
    if selected_series:
        for serie in selected_series:
            years_raw = Medicao.objects.filter(serie=serie).annotate(
                year=ExtractYear('timestamp')
            ).values_list('year', flat=True).distinct().order_by('year')
            
            years_processed = MedicaoProcessada.objects.filter(serie=serie).annotate(
                year=ExtractYear('timestamp')
            ).values_list('year', flat=True).distinct().order_by('year')
            
            all_serie_years = set(years_raw) | set(years_processed)
            series_all_years[str(serie.id)] = sorted(list(all_serie_years))
    elif selected_ponto_medicao:
        for serie in series_for_point:
            years_raw = Medicao.objects.filter(serie=serie).annotate(
                year=ExtractYear('timestamp')
            ).values_list('year', flat=True).distinct().order_by('year')
            
            years_processed = MedicaoProcessada.objects.filter(serie=serie).annotate(
                year=ExtractYear('timestamp')
            ).values_list('year', flat=True).distinct().order_by('year')
            
            all_serie_years = set(years_raw) | set(years_processed)
            series_all_years[str(serie.id)] = sorted(list(all_serie_years))

    series_data = {} 
    all_years = set()
    if series_all_years:
        for serie_years_list in series_all_years.values():
            all_years.update(serie_years_list)
    else:
        all_years_raw = Medicao.objects.filter(
            serie__ponto_medida__user=request.user
        ).annotate(year=ExtractYear('timestamp')).values_list('year', flat=True).distinct()
        all_years_processed = MedicaoProcessada.objects.filter(
            serie__ponto_medida__user=request.user
        ).annotate(year=ExtractYear('timestamp')).values_list('year', flat=True).distinct()
        all_years = set(all_years_raw) | set(all_years_processed)
    
    selected_year_final = None
    if selected_year:
        try:
            selected_year_final = int(selected_year)
        except ValueError:
            selected_year_final = None

    for selected_serie in selected_series:
        
        if comparison_mode and str(selected_serie.id) in series_years:
            selected_years_for_serie = series_years[str(selected_serie.id)]
            
            for year_to_process in selected_years_for_serie:
                years, counts, totals, avg_values = [], [], [], []
                month_labels, month_counts, month_totals, month_avg = [], [], [], []
                
                
                if data_type == 'raw':
                    
                    estatisticas_anuais = EstatisticaAnual.objects.filter(
                        serie=selected_serie,
                        metodo=data_type,
                        ano=year_to_process
                    )

                    if estatisticas_anuais.exists():
                        for e in estatisticas_anuais:
                            years.append(e.ano)
                            totals.append(e.total)
                            counts.append(e.contagem)
                            avg_values.append(e.media)
                    else:       
                        yearly_data = Medicao.objects.filter(
                            serie=selected_serie,
                            timestamp__year=year_to_process
                        ).aggregate(
                            total_valor=Sum('valor'), 
                            count=Count('id'), 
                            avg_valor=Avg('valor')
                        )

                        if yearly_data['count']:
                            years = [year_to_process]
                            counts = [yearly_data['count']]
                            totals = [yearly_data['total_valor'] or 0]
                            avg_values = [round(yearly_data['avg_valor'] or 0, 2)]
                    
                    all_years.update(years)
                    
                    
                    estatisticas_mensais = EstatisticaMensal.objects.filter(
                        serie=selected_serie,
                        ano=year_to_process,
                        metodo=data_type
                    )
                    
                    if estatisticas_mensais.exists():
                        month_data = {e.mes: e for e in estatisticas_mensais}
                        for m in range(1, 13):
                            month_labels.append(m)
                            if m in month_data:
                                e = month_data[m]
                                month_counts.append(e.contagem)
                                month_totals.append(e.total)
                                month_avg.append(e.media)
                            else:
                                month_counts.append(0)
                                month_totals.append(0)
                                month_avg.append(0)
                    else:
                        monthly_data = Medicao.objects.filter(
                            serie=selected_serie, 
                            timestamp__year=year_to_process
                        ).annotate(
                            month=ExtractMonth('timestamp')
                        ).values('month').annotate(
                            total_valor=Sum('valor'), 
                            count=Count('id'), 
                            avg_valor=Avg('valor')
                        ).order_by('month')

                        monthly_lookup = {entry['month']: entry for entry in monthly_data}
                        for m in range(1, 13):
                            month_labels.append(m)
                            if m in monthly_lookup:
                                entry = monthly_lookup[m]
                                month_counts.append(entry['count'])
                                month_totals.append(entry['total_valor'])
                                month_avg.append(round(entry['avg_valor'], 2))
                            else:
                                month_counts.append(0)
                                month_totals.append(0)
                                month_avg.append(0)
                
                elif data_type == 'normalized':
                    
                    estatisticas_anuais = EstatisticaAnual.objects.filter(
                        serie=selected_serie,
                        metodo=data_type,
                        ano=year_to_process
                    )

                    if estatisticas_anuais.exists():
                        for e in estatisticas_anuais:
                            years.append(e.ano)
                            totals.append(e.total)
                            counts.append(e.contagem)
                            avg_values.append(e.media)
                    else:
                        
                        dadosRaw = Medicao.objects.filter(serie=selected_serie, timestamp__year=year_to_process)
                        df = pd.DataFrame(list(dadosRaw.values('timestamp', 'valor')))
                        
                        if not df.empty:
                            
                            dados_guardados = MedicaoProcessada.objects.filter(
                                serie=selected_serie,
                                metodo='normalized',
                                timestamp__year=year_to_process
                            ).order_by('timestamp')

                            if dados_guardados.exists():
                                df = pd.DataFrame(list(dados_guardados.values('timestamp', 'valor')))
                                df.set_index('timestamp', inplace=True)
                                resampled_df = df 
                            else:
                                df['timestamp'] = pd.to_datetime(df['timestamp'])
                                df.set_index('timestamp', inplace=True)
                                df.index = df.index.tz_localize(None)
                                resampled_df = df.resample('15T').asfreq()
                                
                                year_end = df.index.max().year
                                month_end = df.index.max().month
                                last_day = calendar.monthrange(year_end, month_end)[1]

                                start_date = pd.Timestamp(f"{df.index.min().year}-{df.index.min().month}-01")
                                end_date = pd.Timestamp(f"{year_end}-{month_end}-{last_day} 23:45:00")
                                full_range = pd.date_range(start=start_date, end=end_date, freq='15T')
                                resampled_df = df.resample('15T').asfreq()
                                resampled_df = resampled_df.reindex(full_range)
                                normalize(df, resampled_df, 15)
                                guardaProcessados(resampled_df['valor'].items(), 'normalized', selected_serie)

                            
                            yearly_normalized = resampled_df.groupby(resampled_df.index.year).agg(
                                total_valor=('valor', 'sum'),
                                count=('valor', 'count'),
                                avg_valor=('valor', 'mean')
                            )

                            if not yearly_normalized.empty:
                                years = [year_to_process]
                                totals = [yearly_normalized['total_valor'].iloc[0]]
                                counts = [yearly_normalized['count'].iloc[0]]
                                avg_values = [round(yearly_normalized['avg_valor'].iloc[0], 2)]
                    
                    all_years.update(years)
                    
                    
                    estatisticas_mensais = EstatisticaMensal.objects.filter(
                        serie=selected_serie,
                        ano=year_to_process,
                        metodo=data_type
                    )
                    
                    if estatisticas_mensais.exists():
                        month_data = {e.mes: e for e in estatisticas_mensais}
                        for m in range(1, 13):
                            month_labels.append(m)
                            if m in month_data:
                                e = month_data[m]
                                month_counts.append(e.contagem)
                                month_totals.append(e.total)
                                month_avg.append(e.media)
                            else:
                                month_counts.append(0)
                                month_totals.append(0)
                                month_avg.append(0)
                    else:
                        
                        dados_processados = MedicaoProcessada.objects.filter(
                            serie=selected_serie,
                            metodo='normalized',
                            timestamp__year=year_to_process
                        ).annotate(
                            month=ExtractMonth('timestamp')
                        ).values('month').annotate(
                            total_valor=Sum('valor'), 
                            count=Count('id'), 
                            avg_valor=Avg('valor')
                        ).order_by('month')

                        monthly_lookup = {entry['month']: entry for entry in dados_processados}
                        for m in range(1, 13):
                            month_labels.append(m)
                            if m in monthly_lookup:
                                entry = monthly_lookup[m]
                                month_counts.append(entry['count'])
                                month_totals.append(entry['total_valor'])
                                month_avg.append(round(entry['avg_valor'], 2))
                            else:
                                month_counts.append(0)
                                month_totals.append(0)
                                month_avg.append(0)

                elif data_type == 'reconstruido':
                    
                    estatisticas_anuais = EstatisticaAnual.objects.filter(
                        serie=selected_serie,
                        metodo=recon_method,
                        ano=year_to_process
                    )

                    if estatisticas_anuais.exists():
                        for e in estatisticas_anuais:
                            years.append(e.ano)
                            totals.append(e.total)
                            counts.append(e.contagem)
                            avg_values.append(e.media)
                    else:
                        
                        with localconverter(default_converter + pandas2ri.converter):
                            robjects.r.source(R_SCRIPT_PATH)
                        
                        dados_raw = Medicao.objects.filter(serie=selected_serie, timestamp__year=year_to_process)
                        df = pd.DataFrame(list(dados_raw.values('timestamp', 'valor')))

                        
                        dados_guardados = MedicaoProcessada.objects.filter(
                            serie=selected_serie,
                            metodo=recon_method,
                            timestamp__year=year_to_process
                        ).order_by('timestamp')

                        if dados_guardados.exists():
                            df = pd.DataFrame(list(dados_guardados.values('timestamp', 'valor')))
                            df.set_index('timestamp', inplace=True)
                            resampled_df = df
                        else:
                            if not df.empty:
                                df['timestamp'] = pd.to_datetime(df['timestamp'])
                                df.set_index('timestamp', inplace=True)
                                df.index = df.index.tz_localize(None)
                                resampled_df = df.resample('15T').asfreq()
                                year_end = df.index.max().year
                                month_end = df.index.max().month
                                last_day = calendar.monthrange(year_end, month_end)[1]

                                start_date = pd.Timestamp(f"{df.index.min().year}-{df.index.min().month}-01")
                                end_date = pd.Timestamp(f"{year_end}-{month_end}-{last_day} 23:45:00")
                                full_range = pd.date_range(start=start_date, end=end_date, freq='15T')
                                resampled_df = df.resample('15T').asfreq()
                                resampled_df = resampled_df.reindex(full_range)
                                resampled_df.index.name = 'Date'
                                normalize(df, resampled_df, 15)
                                resampled_df['Date'] = resampled_df.index.strftime('%Y/%m/%d')
                                resampled_df['Time'] = resampled_df.index.strftime('%H:%M')
                            
                                matrix_df = resampled_df.pivot(index='Date', columns='Time', values='valor')
                                matrix_df.reset_index()
                            
                                matrix_df.columns.name = None
                                matrix_pronta = matrix_df.reset_index()
                                with localconverter(default_converter + pandas2ri.converter):
                                    robjects.globalenv['matrix_pronta'] = pandas2ri.py2rpy(matrix_pronta)

                                reconstructed_values_list = []
                                if recon_method == 'jq':
                                    JQ_function = robjects.globalenv['JQ.function']
                                    reconstructedValues = JQ_function()
                                    reconstructed_values_list = reconstructedValues.tolist()
                                else:
                                    TBATS_function = robjects.globalenv['TBATS.function']
                                    reconstructedValues = TBATS_function()
                                    reconstructed_values_list = reconstructedValues.tolist()
                            
                                resampled_df['valor'] = reconstructed_values_list
                                guardaProcessados(resampled_df['valor'].items(), recon_method, selected_serie)

                        
                        yearly_reconstructed = resampled_df.groupby(resampled_df.index.year).agg(
                            total_valor=('valor', 'sum'),
                            count=('valor', 'count'),
                            avg_valor=('valor', 'mean')
                        )

                        if not yearly_reconstructed.empty:
                            years = [year_to_process]
                            totals = [yearly_reconstructed['total_valor'].iloc[0]]
                            counts = [yearly_reconstructed['count'].iloc[0]]
                            avg_values = [round(yearly_reconstructed['avg_valor'].iloc[0], 2)]
                    
                    all_years.update(years)
                    
                    
                    estatisticas_mensais = EstatisticaMensal.objects.filter(
                        serie=selected_serie,
                        ano=year_to_process,
                        metodo=recon_method
                    )
                    
                    if estatisticas_mensais.exists():
                        month_data = {e.mes: e for e in estatisticas_mensais}
                        for m in range(1, 13):
                            month_labels.append(m)
                            if m in month_data:
                                e = month_data[m]
                                month_counts.append(e.contagem)
                                month_totals.append(e.total)
                                month_avg.append(e.media)
                            else:
                                month_counts.append(0)
                                month_totals.append(0)
                                month_avg.append(0)
                    else:
                        
                        dados_processados = MedicaoProcessada.objects.filter(
                            serie=selected_serie,
                            metodo=recon_method,
                            timestamp__year=year_to_process
                        ).annotate(
                            month=ExtractMonth('timestamp')
                        ).values('month').annotate(
                            total_valor=Sum('valor'), 
                            count=Count('id'), 
                            avg_valor=Avg('valor')
                        ).order_by('month')

                        monthly_lookup = {entry['month']: entry for entry in dados_processados}
                        for m in range(1, 13):
                            month_labels.append(m)
                            if m in monthly_lookup:
                                entry = monthly_lookup[m]
                                month_counts.append(entry['count'])
                                month_totals.append(entry['total_valor'])
                                month_avg.append(round(entry['avg_valor'], 2))
                            else:
                                month_counts.append(0)
                                month_totals.append(0)
                                month_avg.append(0)

                
                serie_year_key = f"{selected_serie.id}_{year_to_process}"
                
                series_data[serie_year_key] = {
                    'serie': {
                        'id': int(selected_serie.id), 
                        'nome': str(selected_serie.nome)
                    },
                    'selected_year': int(year_to_process),
                    'years': [int(y) for y in years],
                    'all_available_years': series_all_years.get(str(selected_serie.id), []),
                    'counts': [int(c) for c in counts],
                    'totals': [float(t) for t in totals],
                    'avg_values': [float(a) for a in avg_values],
                    'month_labels': [int(m) for m in month_labels],
                    'month_counts': [int(c) for c in month_counts],
                    'month_totals': [float(t) for t in month_totals],
                    'month_avg': [float(a) for a in month_avg]
                }
        else:
            
            years, counts, totals, avg_values = [], [], [], []
            month_labels, month_counts, month_totals, month_avg = [], [], [], []

            
            if data_type == 'raw':
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
                    yearly_data = Medicao.objects.filter(serie=selected_serie).annotate(
                    year=ExtractYear('timestamp')
                    ).values('year').annotate(
                    total_valor=Sum('valor'), count=Count('id'), avg_valor=Avg('valor')
                    ).order_by('year')

                    years = [entry['year'] for entry in yearly_data]
                    counts = [entry['count'] for entry in yearly_data]
                    totals = [entry['total_valor'] for entry in yearly_data]
                    avg_values = [round(entry['avg_valor'], 2) for entry in yearly_data]
                    guardaEstatisticaAnual(zip(years, totals, counts, avg_values),data_type,selected_serie)
                
                all_years.update(years)
                
                if not selected_year_final:
                    if years:
                        selected_year_final = max(years)
                    else:
                        selected_year_final = None
                
                
                year_for_monthly = selected_year_final
                if year_for_monthly:
                    estatisticas_mensais = EstatisticaMensal.objects.filter(
                        serie=selected_serie,
                        ano=year_for_monthly,
                        metodo=data_type
                    )
                    
                    if estatisticas_mensais.exists():
                        month_data = {e.mes: e for e in estatisticas_mensais}
                        for m in range(1, 13):
                            month_labels.append(m)
                            if m in month_data:
                                e = month_data[m]
                                month_counts.append(e.contagem)
                                month_totals.append(e.total)
                                month_avg.append(e.media)
                            else:
                                month_counts.append(0)
                                month_totals.append(0)
                                month_avg.append(0)
                    else:
                        monthly_data = Medicao.objects.filter(
                            serie=selected_serie, timestamp__year=year_for_monthly
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
                                month_avg.append(round(entry['avg_valor'], 2))
                            else:
                                month_counts.append(0)
                                month_totals.append(0)
                                month_avg.append(0)

            elif data_type == 'normalized':
                dadosRaw = Medicao.objects.filter(serie=selected_serie)
                df = pd.DataFrame(list(dadosRaw.values('timestamp', 'valor')))
                dados_guardados = MedicaoProcessada.objects.filter(
                serie=selected_serie,
                metodo='normalized'
                 ).order_by('timestamp')

                if dados_guardados.exists():
                    df = pd.DataFrame(list(dados_guardados.values('timestamp', 'valor')))
                    df.set_index('timestamp', inplace=True)
                    resampled_df = df 
                else:
                    if not df.empty:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df.set_index('timestamp', inplace=True)
                        df.index = df.index.tz_localize(None)
                        resampled_df = df.resample('15T').asfreq()
                        
                        year_end = df.index.max().year
                        month_end = df.index.max().month
                        last_day = calendar.monthrange(year_end, month_end)[1]

                        start_date = pd.Timestamp(f"{df.index.min().year}-{df.index.min().month}-01")
                        end_date = pd.Timestamp(f"{year_end}-{month_end}-{last_day} 23:45:00")
                        full_range = pd.date_range(start=start_date, end=end_date, freq='15T')
                        resampled_df = df.resample('15T').asfreq()
                        resampled_df = resampled_df.reindex(full_range)
                        normalize(df,resampled_df, 15)
                        guardaProcessados(resampled_df['valor'].items(),'normalized',selected_serie)

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
                    avg_values = [round(x, 2) for x in yearly_normalized['avg_valor'].tolist()]
                    guardaEstatisticaAnual(zip(years, totals, counts, avg_values),data_type,selected_serie)

                all_years.update(years)

                if not selected_year_final:
                    if years:
                        selected_year_final = max(years)
                    else:
                        selected_year_final = None
                
                
                year_for_monthly = selected_year_final
                if year_for_monthly:
                    estatisticas_mensais = EstatisticaMensal.objects.filter(
                        serie=selected_serie,
                        ano=year_for_monthly,
                        metodo=data_type
                    )
                    
                    if estatisticas_mensais.exists():
                        month_data = {e.mes: e for e in estatisticas_mensais}
                        for m in range(1, 13):
                            month_labels.append(m)
                            if m in month_data:
                                e = month_data[m]
                                month_counts.append(e.contagem)
                                month_totals.append(e.total)
                                month_avg.append(e.media)
                            else:
                                month_counts.append(0)
                                month_totals.append(0)
                                month_avg.append(0)
                    else:
                        if year_for_monthly:
                            resampled_df_selected_year = resampled_df[resampled_df.index.year == year_for_monthly]
                            monthly_normalized = resampled_df_selected_year.groupby(resampled_df_selected_year.index.month).agg(
                                count=('valor', 'count'),
                                total_valor=('valor', 'sum'),
                                avg_valor=('valor', 'mean')
                            ).reindex(range(1, 13), fill_value=0)

                            month_counts = [int(x) if pd.notnull(x) and not math.isnan(x) else 0
                                           for x in monthly_normalized['count'].tolist()
                                           ]

                            month_totals = [float(x) if pd.notnull(x) and not math.isnan(x) else 0.0
                                            for x in monthly_normalized['total_valor'].tolist()
                                            ]

                            month_avg = [round(x, 2) if pd.notnull(x) and not math.isnan(x) else 0.0
                                         for x in monthly_normalized['avg_valor'].tolist()
                                        ]

                            month_labels = [i for i in range(1, 13)]
                            
                            if year_for_monthly:
                                serie_data_monthly = MedicaoProcessada.objects.filter(
                                    serie=selected_serie, 
                                    metodo=recon_method, 
                                    timestamp__year=year_for_monthly
                                ).order_by('timestamp')
                                boxplot_data_monthly = calculate_boxplot_data(
                                    serie_data_monthly, selected_serie, recon_method, year_for_monthly, True
                                )
                                
                                dados_completos = []
                                for i, mes in enumerate(month_labels):
                                    if mes in boxplot_data_monthly:
                                        dados_completos.append((
                                            mes,
                                            month_totals[i],
                                            month_counts[i],
                                            month_avg[i],
                                            boxplot_data_monthly[mes].get('min', 0.0),
                                            boxplot_data_monthly[mes].get('max', 0.0),
                                            boxplot_data_monthly[mes].get('median', 0.0),
                                            boxplot_data_monthly[mes].get('q1', 0.0),
                                            boxplot_data_monthly[mes].get('q3', 0.0),
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
                                guardaEstatisticaMensal(dados_completos, recon_method, selected_serie, year_for_monthly)
                            else:
                                
                                guardaEstatisticaMensal(
                                    zip(month_labels, month_totals, month_counts, month_avg, 
                                        [0.0]*12, [0.0]*12, [0.0]*12, [0.0]*12, [0.0]*12),
                                    recon_method, selected_serie, year_for_monthly
                                )

            elif data_type == 'reconstruido':
                with localconverter(default_converter + pandas2ri.converter):
                    robjects.r.source(R_SCRIPT_PATH)
                
                dados_raw = Medicao.objects.filter(serie=selected_serie)
                df = pd.DataFrame(list(dados_raw.values('timestamp', 'valor')))

                dados_guardados = MedicaoProcessada.objects.filter(serie=selected_serie,
                metodo=recon_method
                 ).order_by('timestamp')

                if dados_guardados.exists():
                    df = pd.DataFrame(list(dados_guardados.values('timestamp', 'valor')))
                    df.set_index('timestamp', inplace=True)
                    resampled_df = df
                        
                else:
                    if not df.empty:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df.set_index('timestamp', inplace=True)
                        df.index = df.index.tz_localize(None)
                        resampled_df = df.resample('15T').asfreq()
                        year_end = df.index.max().year
                        month_end = df.index.max().month
                        last_day = calendar.monthrange(year_end, month_end)[1]

                        start_date = pd.Timestamp(f"{df.index.min().year}-{df.index.min().month}-01")
                        end_date = pd.Timestamp(f"{year_end}-{month_end}-{last_day} 23:45:00")
                        full_range = pd.date_range(start=start_date, end=end_date, freq='15T')
                        resampled_df = df.resample('15T').asfreq()
                        resampled_df = resampled_df.reindex(full_range)
                        resampled_df.index.name = 'Date'
                        normalize(df,resampled_df, 15)
                        resampled_df['Date'] = resampled_df.index.strftime('%Y/%m/%d')
                        resampled_df['Time'] = resampled_df.index.strftime('%H:%M')
                    
                        matrix_df = resampled_df.pivot(index='Date', columns='Time', values='valor')
                        matrix_df.reset_index()
                    
                        matrix_df.columns.name = None
                        matrix_pronta =matrix_df.reset_index()
                        with localconverter(default_converter + pandas2ri.converter):
                            robjects.globalenv['matrix_pronta'] = pandas2ri.py2rpy(matrix_pronta)

                        reconstructed_values_list=[]
                        if recon_method == 'jq':
                            JQ_function = robjects.globalenv['JQ.function']
                            reconstructedValues = JQ_function()
                            reconstructed_values_list = reconstructedValues.tolist()

                        else:
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
                    
                    yearly_reconstructed = resampled_df.groupby(resampled_df.index.year).agg(
                        total_valor=('valor', 'sum'),
                        count=('valor', 'count'),
                        avg_valor=('valor', 'mean')
                        )

                    years = yearly_reconstructed.index.tolist()
                    totals = yearly_reconstructed['total_valor'].tolist()
                    counts = yearly_reconstructed['count'].tolist()
                    avg_values = [round(x, 2) for x in yearly_reconstructed['avg_valor'].tolist()]
                    guardaEstatisticaAnual(zip(years, totals, counts, avg_values),recon_method,selected_serie)
                
                all_years.update(years)
                
                if not selected_year_final:
                    if years:
                        selected_year_final = max(years)
                    else:
                        selected_year_final = None

                
                year_for_monthly = selected_year_final
                if year_for_monthly:
                    estatisticas_mensais = EstatisticaMensal.objects.filter(
                        serie=selected_serie,
                        ano=year_for_monthly,
                        metodo=recon_method
                    )
                    
                    if estatisticas_mensais.exists():
                        month_data = {e.mes: e for e in estatisticas_mensais}
                        for m in range(1, 13):
                            month_labels.append(m)
                            if m in month_data:
                                e = month_data[m]
                                month_counts.append(e.contagem)
                                month_totals.append(e.total)
                                month_avg.append(e.media)
                            else:
                                month_counts.append(0)
                                month_totals.append(0)
                                month_avg.append(0)
                    else:
                        if year_for_monthly:
                            resampled_df_selected_year = resampled_df[resampled_df.index.year == year_for_monthly]
                            monthly_reconstructed = resampled_df_selected_year.groupby(resampled_df_selected_year.index.month).agg(
                                count=('valor', 'count'),
                                total_valor=('valor', 'sum'),
                                avg_valor=('valor', 'mean')
                                ).reindex(range(1, 13), fill_value=0)

                            month_counts = [int(x) if pd.notnull(x) and not math.isnan(x) else 0
                                           for x in monthly_reconstructed['count'].tolist()
                                           ]

                            month_totals = [float(x) if pd.notnull(x) and not math.isnan(x) else 0.0
                                            for x in monthly_reconstructed['total_valor'].tolist()
                                            ]

                            month_avg = [round(x, 2) if pd.notnull(x) and not math.isnan(x) else 0.0
                                         for x in monthly_reconstructed['avg_valor'].tolist()
                                        ]
                            month_labels = [i for i in range(1, 13)]
                            
                            if year_for_monthly:
                                serie_data_monthly = MedicaoProcessada.objects.filter(
                                    serie=selected_serie, 
                                    metodo=recon_method, 
                                    timestamp__year=year_for_monthly
                                ).order_by('timestamp')
                                boxplot_data_monthly = calculate_boxplot_data(
                                    serie_data_monthly, selected_serie, recon_method, year_for_monthly, True
                                )
                                
                                dados_completos = []
                                for i, mes in enumerate(month_labels):
                                    if mes in boxplot_data_monthly:
                                        dados_completos.append((
                                            mes,
                                            month_totals[i],
                                            month_counts[i],
                                            month_avg[i],
                                            boxplot_data_monthly[mes].get('min', 0.0),
                                            boxplot_data_monthly[mes].get('max', 0.0),
                                            boxplot_data_monthly[mes].get('median', 0.0),
                                            boxplot_data_monthly[mes].get('q1', 0.0),
                                            boxplot_data_monthly[mes].get('q3', 0.0),
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
                                guardaEstatisticaMensal(dados_completos, recon_method, selected_serie, year_for_monthly)
                            else:
                                
                                guardaEstatisticaMensal(
                                    zip(month_labels, month_totals, month_counts, month_avg, 
                                        [0.0]*12, [0.0]*12, [0.0]*12, [0.0]*12, [0.0]*12),
                                    recon_method, selected_serie, year_for_monthly
                                )

            series_data[selected_serie.id] = {
                'serie': {'id': int(selected_serie.id), 'nome': str(selected_serie.nome)},
                'selected_year': int(selected_year_final) if selected_year_final else 'N/A',
                'years': [int(y) for y in years],
                'all_available_years': series_all_years.get(str(selected_serie.id), []),
                'counts': [int(c) for c in counts],
                'totals': [float(t) for t in totals],
                'avg_values': [float(a) for a in avg_values],
                'month_labels': [int(m) for m in month_labels],
                'month_counts': [int(c) for c in month_counts],
                'month_totals': [float(t) for t in month_totals],
                'month_avg': [float(a) for a in month_avg]
            }

    if not selected_year_final and all_years:
        selected_year_final = max(all_years)

    
    boxplot_data = {}
    line_chart_data = {}
    comparison_boxplot_data = {}
    comparison_line_chart_data = {}
    
    if selected_series and len(selected_series) > 0:
        if comparison_mode and len(selected_series) > 1:
            
            for selected_serie in selected_series:
                if str(selected_serie.id) in series_years:
                    selected_years_for_serie = series_years[str(selected_serie.id)]
                    for year_to_process in selected_years_for_serie:
                        serie_year_key = f"{selected_serie.id}_{year_to_process}"
                        
                        if data_type == 'raw':
                            queryset = Medicao.objects.filter(serie=selected_serie, timestamp__year=year_to_process)
                        elif data_type == 'normalized':
                            queryset = MedicaoProcessada.objects.filter(
                                serie=selected_serie, 
                                metodo='normalized', 
                                timestamp__year=year_to_process
                            )
                        elif data_type == 'reconstruido':
                            queryset = MedicaoProcessada.objects.filter(
                                serie=selected_serie, 
                                metodo=recon_method, 
                                timestamp__year=year_to_process
                            )
                        else:
                            queryset = Medicao.objects.none()
                        
                        
                        serie_boxplot_data = calculate_boxplot_data(
                            queryset, selected_serie, 
                            data_type if data_type != 'reconstruido' else recon_method, 
                            year_to_process, True
                        )
                        comparison_boxplot_data[serie_year_key] = {
                            'data': serie_boxplot_data,
                            'serie_name': selected_serie.nome,
                            'year': year_to_process
                        }
                        
                        
                        serie_line_data = calculate_daily_line_data(queryset)
                        comparison_line_chart_data[serie_year_key] = {
                            'data': serie_line_data,
                            'serie_name': selected_serie.nome,
                            'year': year_to_process
                        }
        else:
            
            first_serie = selected_series[0]
            year_for_charts = selected_year_final
            
            if data_type == 'raw':
                if year_for_charts:
                    queryset = Medicao.objects.filter(serie=first_serie, timestamp__year=year_for_charts)
                else:
                    queryset = Medicao.objects.filter(serie=first_serie)
            elif data_type == 'normalized':
                if year_for_charts:
                    queryset = MedicaoProcessada.objects.filter(
                        serie=first_serie, 
                        metodo='normalized', 
                        timestamp__year=year_for_charts
                    )
                else:
                    queryset = MedicaoProcessada.objects.filter(serie=first_serie, metodo='normalized')
            elif data_type == 'reconstruido':
                if year_for_charts:
                    queryset = MedicaoProcessada.objects.filter(
                        serie=first_serie, 
                        metodo=recon_method, 
                        timestamp__year=year_for_charts
                    )
                else:
                    queryset = MedicaoProcessada.objects.filter(serie=first_serie, metodo=recon_method)
            else:
                queryset = Medicao.objects.none()
            
            
            boxplot_data = calculate_boxplot_data(queryset, first_serie, data_type if data_type != 'reconstruido' else recon_method, year_for_charts, True)
            line_chart_data = calculate_daily_line_data(queryset)
            
            
            if data_type == 'normalized' or data_type == 'reconstruido':
                
                if data_type == 'normalized':
                    serie_dataT = MedicaoProcessada.objects.filter(serie=first_serie, metodo='normalized').order_by('timestamp')
                    serie_data_year = MedicaoProcessada.objects.filter(serie=first_serie, metodo='normalized', timestamp__year=year_for_charts).order_by('timestamp')
                else:  # reconstruido
                    serie_dataT = MedicaoProcessada.objects.filter(serie=first_serie, metodo=recon_method).order_by('timestamp')
                    serie_data_year = MedicaoProcessada.objects.filter(serie=first_serie, metodo=recon_method, timestamp__year=year_for_charts).order_by('timestamp')
                
                
                dados_gragico_linhas_instantesT = dadosGraficoTodosInstantes(serie_dataT)
                labels_grafico_linhasT = dados_gragico_linhas_instantesT["labels"]
                valores_grafico_linhasT = dados_gragico_linhas_instantesT["valores"]
                
                
                dados_gragico_linhas = dadosGraficoLinhas(serie_data_year)
                labels_grafico_linhas = dados_gragico_linhas["labels"]
                valores_grafico_linhas = dados_gragico_linhas["valores"]
            elif data_type == 'raw':
                
                serie_dataT = Medicao.objects.filter(serie=first_serie).order_by('timestamp')
                dados_gragico_linhas_instantesT = dadosGraficoTodosInstantes(serie_dataT)
                labels_grafico_linhasT = dados_gragico_linhas_instantesT["labels"]
                valores_grafico_linhasT = dados_gragico_linhas_instantesT["valores"]

    month_names=['Janeiro','Fevereiro','Março','Abril','Maio','Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']
    
    context = {
        'pontos_medicao': pontos_medicao,
        'series': series,
        'series_for_point': series_for_point,
        'selected_ponto_medicao': selected_ponto_medicao,
        'selected_series': selected_series,
        'selected_serie_ids': [str(sid) for sid in selected_serie_ids],
        'comparison_mode': comparison_mode,
        'series_data': series_data,
        'series_all_years': series_all_years,
        'series_years': series_years,  
        'all_years': [int(y) for y in sorted(all_years)] if all_years else [],
        'selected_year': int(selected_year_final) if selected_year_final else None,
        'data_type': data_type,  
        'month_names': month_names,
        'recon_method': recon_method,
        'years': series_data[list(series_data.keys())[0]]['years'] if len(series_data) >= 1 else [],
        'counts': series_data[list(series_data.keys())[0]]['counts'] if len(series_data) >= 1 else [],
        'totals': series_data[list(series_data.keys())[0]]['totals'] if len(series_data) >= 1 else [],
        'avg_values': series_data[list(series_data.keys())[0]]['avg_values'] if len(series_data) >= 1 else [],
        'month_labels': series_data[list(series_data.keys())[0]]['month_labels'] if len(series_data) >= 1 else [],
        'month_counts': series_data[list(series_data.keys())[0]]['month_counts'] if len(series_data) >= 1 else [],
        'month_totals': series_data[list(series_data.keys())[0]]['month_totals'] if len(series_data) >= 1 else [],
        'month_avg': series_data[list(series_data.keys())[0]]['month_avg'] if len(series_data) >= 1 else [],
        'boxplot_data': boxplot_data,
        'line_chart_data': line_chart_data,
        'comparison_boxplot_data': comparison_boxplot_data,
        'comparison_line_chart_data': comparison_line_chart_data,
        'linha_temporal_labels': labels_grafico_linhas if 'labels_grafico_linhas' in locals() else [],
        'linha_temporal_valores': valores_grafico_linhas if 'valores_grafico_linhas' in locals() else [],
        'linha_temporal_labelsT': labels_grafico_linhasT if 'labels_grafico_linhasT' in locals() else [],
        'linha_temporal_valoresT': valores_grafico_linhasT if 'valores_grafico_linhasT' in locals() else [],
    }

    return render(request, 'caudais/dashboard.html', context)


@login_required(login_url='/autenticacao/login/')
def exportar_excel(request):
    serie_ids = request.GET.getlist('serie_ids')
    serie_id = request.GET.get('serie_id')  
    data_type = request.GET.get('data_type', 'raw')
    metodo = request.GET.get('recon_method', 'jq')
    
    
    series_years = {}
    for param_name, param_values in request.GET.lists():
        if param_name.startswith('years_') and param_values:
            try:
                serie_id_from_param = param_name.replace('years_', '')
                selected_years = [int(year) for year in param_values if year]
                if selected_years:
                    series_years[serie_id_from_param] = selected_years
            except ValueError:
                pass

    if serie_id and not serie_ids:
        serie_ids = [serie_id]

    if not serie_ids:
        return HttpResponse("Série não especificada.", status=400)

    try:
        series = [Serie.objects.get(id=sid) for sid in serie_ids if sid]
    except Serie.DoesNotExist:
        return HttpResponse("Série inválida.", status=404)

    if len(series) == 1:
        
        serie = series[0]
        
        
        year_filter = None
        if str(serie.id) in series_years:
            year_filter = series_years[str(serie.id)]
        
        if data_type == 'raw':
            queryset = Medicao.objects.filter(serie=serie)
            if year_filter:
                queryset = queryset.filter(timestamp__year__in=year_filter)
            queryset = queryset.values('timestamp', 'valor')
        elif data_type == 'normalized':
            queryset = MedicaoProcessada.objects.filter(serie=serie, metodo='normalized')
            if year_filter:
                queryset = queryset.filter(timestamp__year__in=year_filter)
            queryset = queryset.values('timestamp', 'valor')
        elif data_type == 'reconstruido':
            queryset = MedicaoProcessada.objects.filter(serie=serie, metodo=metodo)
            if year_filter:
                queryset = queryset.filter(timestamp__year__in=year_filter)
            queryset = queryset.values('timestamp', 'valor')
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
            filename_year = f"_{'_'.join(map(str, year_filter))}" if year_filter else ""
            nome_arquivo = f"medicoes_{data_type}_{metodo}_serie{serie.id}{filename_year}.xlsx"
        else:
            filename_year = f"_{'_'.join(map(str, year_filter))}" if year_filter else ""
            nome_arquivo = f"medicoes_{data_type}_serie{serie.id}{filename_year}.xlsx"

        response['Content-Disposition'] = f'attachment; filename="{nome_arquivo}"'

        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Medições')

        return response
    
    else:
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        
        
        years_str = ""
        if series_years:
            all_years = []
            for years_list in series_years.values():
                all_years.extend(years_list)
            unique_years = sorted(set(all_years))
            years_str = f"_anos{'_'.join(map(str, unique_years))}"
        
        if data_type == "reconstruido":
            nome_arquivo = f"comparacao_{data_type}_{metodo}_series{years_str}.xlsx"
        else:
            nome_arquivo = f"comparacao_{data_type}_series{years_str}.xlsx"
            
        response['Content-Disposition'] = f'attachment; filename="{nome_arquivo}"'

        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            for serie in series:
                
                year_filter = None
                if str(serie.id) in series_years:
                    year_filter = series_years[str(serie.id)]
                
                if data_type == 'raw':
                    queryset = Medicao.objects.filter(serie=serie)
                    if year_filter:
                        queryset = queryset.filter(timestamp__year__in=year_filter)
                    queryset = queryset.values('timestamp', 'valor')
                elif data_type == 'normalized':
                    queryset = MedicaoProcessada.objects.filter(serie=serie, metodo='normalized')
                    if year_filter:
                        queryset = queryset.filter(timestamp__year__in=year_filter)
                    queryset = queryset.values('timestamp', 'valor')
                elif data_type == 'reconstruido':
                    queryset = MedicaoProcessada.objects.filter(serie=serie, metodo=metodo)
                    if year_filter:
                        queryset = queryset.filter(timestamp__year__in=year_filter)
                    queryset = queryset.values('timestamp', 'valor')
                else:
                    continue

                df = pd.DataFrame(list(queryset))
                if not df.empty:
                    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
                    df = df.sort_values(by='timestamp')
                    
                    
                    year_suffix = f"_{'_'.join(map(str, year_filter))}" if year_filter else ""
                    df.rename(columns={'timestamp': 'Data', 'valor': f'Caudal_{serie.nome}{year_suffix}'}, inplace=True)
                    
                    
                    sheet_name_year = f"_{'_'.join(map(str, year_filter))}" if year_filter else ""
                    sheet_name = f"Serie_{serie.id}_{serie.nome}{sheet_name_year}"[:31]  
                    df.to_excel(writer, index=False, sheet_name=sheet_name)

        return response

@login_required
def obter_series_por_ponto(request):
    ponto_id = request.GET.get('ponto_id')
    series_data = []
    
    if ponto_id:
        
        series = Serie.objects.filter(ponto_medida_id=ponto_id).select_related('ponto_medida')
        
        
        series_ids = [serie.id for serie in series]
        
        
        years_raw_bulk = Medicao.objects.filter(
            serie_id__in=series_ids
        ).annotate(
            year=ExtractYear('timestamp')
        ).values('serie_id', 'year').distinct()
        
        
        years_processed_bulk = MedicaoProcessada.objects.filter(
            serie_id__in=series_ids
        ).annotate(
            year=ExtractYear('timestamp')
        ).values('serie_id', 'year').distinct()
        
        
        years_by_serie = {}
        for item in years_raw_bulk:
            serie_id = item['serie_id']
            year = item['year']
            if serie_id not in years_by_serie:
                years_by_serie[serie_id] = set()
            years_by_serie[serie_id].add(year)
            
        for item in years_processed_bulk:
            serie_id = item['serie_id']
            year = item['year']
            if serie_id not in years_by_serie:
                years_by_serie[serie_id] = set()
            years_by_serie[serie_id].add(year)
        
        
        for serie in series:
            all_years = sorted(list(years_by_serie.get(serie.id, set())))
            
            series_data.append({
                'id': serie.id,
                'nome': serie.nome,
                'years': all_years,
                'latest_year': max(all_years) if all_years else None,
                'total_years': len(all_years)
            })
    
    return JsonResponse(series_data, safe=False)


@login_required(login_url='/autenticacao/login/')
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
        
        
        sorted_labels = sorted(list(all_labels))
        
        return JsonResponse({
            'labels': sorted_labels,
            'datasets': datasets
        })
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

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

    valores_limpos = [float(v) for v in valores if v is not None and not pd.isna(v)]
    
    if len(valores_limpos) == 0:
        return []

    valores_np = np.array(valores_limpos)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    outliers_mask = (valores_np < lower_bound) | (valores_np > upper_bound)
    outliers = valores_np[outliers_mask]
    
    return outliers.tolist() if len(outliers) > 0 else []