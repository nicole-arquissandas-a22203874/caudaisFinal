Requisitos:
1-Download da Linguagem R versão 4.4.3, através do link https://cran.r-project.org/
2-Download do miniconda através do link https://www.anaconda.com/docs/getting-started/miniconda/install

Nota:
Nas variaveis de ambiente no Path adicione o caminho do bin do R ,caminho onde foi instalado o miniconda e o caminho para os scripts do miniconda. 
Exemplo no windows
-C:\Program Files\R\R-4.4.3\bin
-C:\ProgramData\miniconda3 
-C:\ProgramData\miniconda3\Scripts

Passos para correr a aplicação:
1-Download do code do github
2-Extrair o zip  que foi downloaded do github
3-Abrir o a pasta  no visual studio code
4-Criação do ambiente virtual (Replicando do ficheiro "environment.yml")
5-Instalação dos pacotes R necessários

Passo a Passo
  1. No terminal, corra o seguinte comando para replicar o nosso ambiente virtual(vai ficar com o nome myenv2):
  conda env create -f environment.yml
  2. Ativa o ambiente virtual:
  conda activate myenv2
  3. Registe o ambiente myenv2 como um kernel de jupyter:
  python -m ipykernel install --user --name=myenv2 --display-name "Python (myenv2)"
  4. Corra o jupyter:
  jupyter notebook
  5. Apos abrir o jupyter selecione o kernel registado "Python (myenv2)" e corra as células 4-26,   para instalação dos pacotes R necessários.
   Nota:nas células com caminhos especificados, altere para o caminho correto onde esta o Library da pasta R dentro do ambiente virtual.
  6. Por fim corra o django (onde está a aplicação)
     python manage.py runserver
     Nota:Na views.py vai ser necessário alterar o caminho do  R script "reconstruction_script.R"

Na Pasta Dados apresentam-se dados de caudal num excell para testar a aplicação








