from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()

class Regiao(models.Model):
    nome = models.CharField(max_length=100)
    localidade = models.CharField(max_length=100)
   

    def __str__(self):
        return f'{self.nome} - {self.localidade}'


class PontoMedida(models.Model):
    regiao = models.ForeignKey(Regiao, on_delete=models.CASCADE,related_name='pontoMedida')
    tipoMedidor=models.CharField(max_length=100)
    latitude = models.FloatField(null=True, blank=True)
    longitude = models.FloatField(null=True, blank=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='pontos_medicao')

    def __str__(self):
        return f'Ponto de Medida {self.id} - {self.regiao}'


class Serie(models.Model):
    ponto_medida = models.ForeignKey(PontoMedida, on_delete=models.CASCADE,related_name='serie')
    nome = models.CharField(max_length=100,null=True, blank=True)
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['ponto_medida', 'nome'],
                name='unique_serie_por_ponto'
            )
        ]

    def __str__(self):
        return f'Série {self.nome} - {self.ponto_medida}'


class Medicao(models.Model):
    serie = models.ForeignKey(Serie, on_delete=models.CASCADE,related_name='medicoes')
    valor = models.FloatField(null=True, blank=True)
    timestamp = models.DateTimeField(null=True, blank=True, db_index=True)   # Indexado para melhorar pesquisa

    def __str__(self):
        return f'{self.serie}-Caudal: {self.valor} - Data: {self.timestamp}'
    class Meta:
        unique_together = ('timestamp','serie')

class MedicaoProcessada(models.Model):
    METODO_CHOICES = [
        ('normalized', 'Normalizado'),
        ('jq', 'Reconstruído (JQ)'),
        ('tbats', 'Reconstruído (TBATS)'),
    ]
    

    serie= models.ForeignKey(Serie, on_delete=models.CASCADE,related_name='medicoes_Processadas',null=True, blank=True)
    metodo = models.CharField(max_length=20, choices=METODO_CHOICES)
    timestamp = models.DateTimeField(db_index=True)
    valor = models.FloatField(null=True, blank=True)
    ano = models.IntegerField()


    class Meta:
        unique_together = ('metodo', 'timestamp','serie')

    def __str__(self):
        return f'{self.serie}{self.metodo.upper()} - {self.timestamp} - {self.valor}'

class EstatisticaAnual(models.Model):
    METODO_CHOICES = [
        ('normalized', 'Normalizado'),
        ('jq', 'Reconstruído (JQ)'),
        ('tbats', 'Reconstruído (TBATS)'),
        ('raw', 'Raw'),
    ]

    serie= models.ForeignKey(Serie, on_delete=models.CASCADE,related_name='estatisticasAnuais',null=True, blank=True)
    metodo = models.CharField(max_length=20, choices=METODO_CHOICES)
    ano = models.IntegerField()
    total = models.FloatField()
    media = models.FloatField()
    contagem = models.IntegerField()

    class Meta:
        unique_together = ( 'metodo', 'ano','serie')

class EstatisticaMensal(models.Model):
    METODO_CHOICES = [
        ('normalized', 'Normalizado'),
        ('jq', 'Reconstruído (JQ)'),
        ('tbats', 'Reconstruído (TBATS)'),
        ('raw', 'Raw'),
    ]

    serie= models.ForeignKey(Serie, on_delete=models.CASCADE,related_name='estatisticasMensais',null=True, blank=True)
    metodo = models.CharField(max_length=20, choices=METODO_CHOICES)
    ano = models.IntegerField()
    mes = models.IntegerField()
    total = models.FloatField()
    media = models.FloatField()
    contagem = models.IntegerField()
    
    # Boxplot statistics fields
    minWhisker = models.FloatField(null=True, blank=True)
    maxWhisker = models.FloatField(null=True, blank=True)
    medianaMensal = models.FloatField(null=True, blank=True)
    q1 = models.FloatField(null=True, blank=True)
    q3 = models.FloatField(null=True, blank=True)

    class Meta:
        unique_together = ('metodo', 'ano', 'mes','serie')

