mport java.io.*;

public class Leitor {


  public static void main(String args[]) throws Exception {

    File dir = new File(".");
    List<String> arqs = new ArrayList<>();

    int K = 10; // numero de funÃ§Ãµes hashes do bloom filter
    int M = 50000;// numero de bits na tabela do bloom filter

    BloomFilter conjuntoDocs = new BloomFilter(K, M)
    BloomFilter palavras = new BloomFilter(2*K, 10*M);

    int maxWindowSize = 10000;
    DGIM contarTendencias = new DGIM(maxWindowSize);

    int iteracoes = 0;
    while (true) {
      iteracoes++;

       for (File arq: dir.listFiles()) {
         if (arq.isFile()) {
             if (arq.getName().matches(".*\\html")) {

               if ( conjuntoDocs.contains(f.getName())) {
                 arq.delete();//
               } else {
		    conjuntoDocs.add(f.getName())
		 }
                 for (String termo: quebrarDocumento(arq)) {
                   if (palavras.contains(termo)) {
                     contador.put(0);
                   } else {
                     palavras.put(termo);
                     contador.put(1);
                   }
                 }
               }
             }
         }
       }

       if (iteracoes % 10 == 0) {
         System.out.println("RelatÃ³rio do fluxo contÃ­nuo de dados.");
         System.out.println("Estimativa de nÃºmero de palavras distintas encontradas: "+ conjuntoDocs.contadorFlajoletMartin());

         System.out.println("Verificador de tendÃªncias: \n");
         for(int t =1 ; t < contador.maxWindowSize();  t=2*t) {
            System.out.println("Na ultima janela de tamanho t == "+t+" foram contadas "+contador.contagemJanela(t)+" novidades");
         }
       }

       Thread.sleep(1000);
       System.out.println("Esperando chegar mais arquivos");
    }
  }

  public static ArrayList<String> quebrarDocumento(File arq) throws Exception{

    BufferedReader br = new BufferedReader(new FileReader(arq));

    String linha ;

    ArrayList<String> termos = new ArrayList<>();

    while ( (linha = br.readLine()) != null ) {
      for (String termo: linha.split(" ")) {
          termos.add(termo);
      }
    }

    return (termos);
  }

}
