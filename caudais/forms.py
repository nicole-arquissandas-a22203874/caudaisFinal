from django import forms
from .models import *
from django.core.exceptions import ValidationError

class RegiaoForm(forms.Form):
    regiao_nome = forms.CharField(
        max_length=100,
        label='Nome',
        widget=forms.TextInput(attrs={'placeholder': 'Insira o nome da região'})
    )
    regiao_localidade = forms.CharField(
        max_length=100,
        label='Localidade',
        widget=forms.TextInput(attrs={'placeholder': 'Insira a localidade da região'})
    )
class PontoMedidaForm(forms.Form):
    tipo_medidor = forms.CharField(
        max_length=100,
        label='Tipo de Medidor',
        widget=forms.TextInput(attrs={'placeholder': 'Insira o tipo de medidor'})
    )
    latitude = forms.FloatField(
        required=False,
        label='Latitude (Opcional)',
        widget=forms.NumberInput(attrs={'placeholder': 'Insira a latitude'})
    )
    longitude = forms.FloatField(
        required=False,
        label='Longitude (Opcional)',
        widget=forms.NumberInput(attrs={'placeholder': 'Insira a longitude '})
    )


class ArquivoExcelForm(forms.Form):
    arquivo_excel = forms.FileField(
        label='Arquivo Excel',
        widget=forms.ClearableFileInput(attrs={'placeholder': 'Selecione um arquivo Excel'})
    )

class SerieNovaComPontoNovoForm(forms.Form):
    nome_serie = forms.CharField(
        max_length=100,
        label="Nome:",
        widget=forms.TextInput(attrs={'placeholder': 'Insira um nome descritivo.Ex:Serie 2013-2015'})
    )
class NovaSerieNoPontoExistenteForm(forms.Form):
    ponto_medida = forms.ModelChoiceField(
        queryset=PontoMedida.objects.all(),
        label="Ponto de Medição",
        empty_label="Selecione um ponto",
        widget=forms.Select(attrs={'class': 'ponto-select'})
    )

    nome_serie = forms.CharField(
        max_length=100,
        label="Nome da Série:",
        widget=forms.TextInput(attrs={'placeholder': 'Ex: Serie 2023-2024'})
    )
    
    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super(NovaSerieNoPontoExistenteForm, self).__init__(*args, **kwargs)
        if user is not None:
            self.fields['ponto_medida'].queryset = PontoMedida.objects.filter(user=user)

    def clean(self):
        cleaned_data = super().clean()
        ponto = cleaned_data.get("ponto_medida")
        nome = cleaned_data.get("nome_serie")

        if ponto and nome:
            if Serie.objects.filter(ponto_medida=ponto, nome=nome).exists():
                self.add_error("nome_serie", "Já existe uma série com este nome para este ponto de medição.")
class AdicionarValoresSerieExistenteForm(forms.Form):
    ponto_medida = forms.ModelChoiceField(
        queryset=PontoMedida.objects.all(),
        label="Ponto de Medição",
        empty_label="Selecione um ponto",
        widget=forms.Select(attrs={'id': 'id_ponto_medida'})
    )

    serie_existente = forms.ModelChoiceField(
        queryset=Serie.objects.all(),
        label="Série Existente",
        empty_label="Selecione uma série existente",
        widget=forms.Select(attrs={'id': 'id_serie_existente'})
    )
    
    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super(AdicionarValoresSerieExistenteForm, self).__init__(*args, **kwargs)
        if user is not None:
            self.fields['ponto_medida'].queryset = PontoMedida.objects.filter(user=user)
            self.fields['serie_existente'].queryset = Serie.objects.filter(ponto_medida__user=user)

class UploadSelectionForm(forms.Form):
    MODO_CHOICE = [
        ("novo", "Criar novo ponto de medição"),
        ("associarSerie", "Criar nova série em ponto existente"),
        ("adicionar_valores", "Adicionar valores a uma série existente")
    ]

    modo = forms.ChoiceField(
        choices=MODO_CHOICE,
        label="Modo de Upload",
        widget=forms.RadioSelect(),  # ← isto define que o campo será um grupo de radio buttons
        
    )
