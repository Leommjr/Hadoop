export HADOOP_CLASSPATH=$(hadoop classpath)
#Criar diretório no dfs
javac -classpath ${HADDOP_CLASSPATH} -d classes Extract.java
jar -cvf extract.jar -C classes .
hadoop jar extract.jar Extract ../data/Input2 ../data/Output

