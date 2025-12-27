"""
nome = input("Digite seu nome")
idade = int( input(" Digite sua idade"))
peso = float( input(" Digite seu peso"))

print(nome)
print(type(idade))
print(type(peso))
"""

"""
salario = float(input("Informe o salario: "))

if salario <= 3000:
    print("programador junior")
elif salario > 3000 and salario <= 6000:
    print("programador pleno")
elif salario > 6000 and salario <= 15000:
    print("programador senior")
else:
    print("gerente de projetos")
    """

#Listas
"""
numeros = [10,9,8,7,6]

print( "total:", len(numeros))
print( "menor valor:", min(numeros))
print( "maior valor:", max(numeros))
"""

#Repetições FOR
"""
notas = []

for x in range(5):
    codigo_aluno = input("RM: ")
    nota = float(input("Nota: "))
    resultado = [codigo_aluno, nota]
    notas.append(resultado)

print( "Quantidade de notas", len(notas))

for n in notas:
    codigo_aluno =n[0]
    nota = n[1]
    print("O RM", codigo_aluno, "tirou a nota:", nota)
"""

#Repetições While
"""
notas = []
contador = 1

while contador <= 5:
    codigo_aluno = input("RM: ")
    nota = float(input("Nota: "))
    resultado = [codigo_aluno, nota]
    notas.append(resultado)

    #Alternativa: contador += 1
    contador = contador +1

print("quantidade de notas", len(notas))

"""

#Dicionarios
"""
# informacoes do jogador
player = {
    "nome": "Guilherme",
    "level": 1,
    "hp": 100,
    "exp": 0,
    "dano": 5,
}

# lista de inimigos
npcs = [
    { "nome": "Monstrinho", "dano": 2, "hp": 50, "exp": 5 },
    { "nome": "Monstro", "dano": 5, "hp": 100, "exp": 10 },
    { "nome": "Monstrão", "dano": 10, "hp": 150, "exp": 15 },
    { "nome": "Chefão", "dano": 25, "hp": 200, "exp": 20 },
]
"""
"""
import os

mensagens = []
nome = input("Nome: ")

while True:
    # Verifica o sistema para usar o comando correto de limpar
    # 'nt' é o nome interno do Windows
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')

    if len(mensagens) > 0:
        for m in mensagens:
            # CORREÇÃO AQUI: m['texto'] agora bate com o append abaixo
            print(m['nome'], "-", m['texto']) 
    
    print("_________")

    texto = input("Mensagem: ")
    if texto == "fim":
        break

    mensagens.append({
        "nome": nome,
        "texto": texto # CORREÇÃO AQUI: chave em minúsculo
    })
"""

#Funções

def minha_funcao(valor1,valor2):
    return valor1 + valor2

resposta = minha_funcao(10,10)

print("resposta", resposta)