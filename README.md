# rabbitmq-common
Arquivos go comuns para os microserviços utilizarem

Benefícios dessa abordagem:
Benefício	Explicação
✅ Contrato único	Todos usam as mesmas estruturas e constantes
✅ Versionamento	Alterações são controladas via tag do pacote
✅ Facilidade de teste	Mock das interfaces para testes unitários
✅ Zero erro de digitação	Constantes evitam strings soltas
✅ Evolução controlada	Novo campo no payload? Só atualizar o pacote
✅ Documentação viva	O próprio código é a documentação


Como gerenciar versões?
# Equipe que mantém o pacote
git tag v1.0.0
git push origin v1.0.0

# Equipe que usa o pacote
go get github.com/sua-empresa/rabbit-common@v1.0.0

# Para atualizar
go get -u github.com/sua-empresa/rabbit-common