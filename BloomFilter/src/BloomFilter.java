import java.util.ArrayList;
import java.math.BigInteger;
import java.util.Random;

// flasolet martin:
// k funcoes hash
// array com k maxtails
// media(mediana de grupos de 2^maxtails)
// media: estimativa do numero de itens no bloom filter
// grupos do tamanho de raiz de 2
public class BloomFilter
{
	private int size;
	private int kHash;
        private ArrayList<int> table;
	private ArrayList<int> randomA;
	private ArrayList<int> primes;
	private ArrayList<int> tails;	
	public void setsize(int m)
	{
        	int cm = int.valueOf(m);
		this.size = cm;
	}
	public void setkHash(int k)
	{
        	int ck = int.valueOf(k);
		this.kHash = ck;
	}
	public int getsize()
	{
        	return this.size;
	}
	public int getkHash()
	{
        	return this.kHash;
	}
	public void settable(int size)
	{
		this.table = new ArrayList<int>;
		for(int i = 0; i<size; i++)
			table.add(0);
	}
	public void setrandomA(int size)
	{
		this.RandomA = new ArrayList<int>();
		for(int i = 0; i<size; i++){
			int a = Random.nextInt(); 
			this.randomA.add(a);
		}
	}
        public void setprimes(int size)
	{
		this.primes = new ArrayList<int>();
		for(int i = 0; i<size; i++){i
        		BigInteger p = new BigInteger();
			p = BigInteger.probablePrime(33, new Random());
			this.primes.add(p); 
		}
	}
	public int gerHash(int hashCode, int pos)
	{
		int b = (randomA[i]*hashCode) & 0x7ffffff;
		return((b%primes[i])%getsize());
				
	}	
	public BloomFilter(int K, int M)
	{
		setsize(M);
		setkHash(K);
		settable(M);
		setrandomA(K);
		setprimes(K);
	}
	public boolean contains(String name)
	{
		for(int i = 0; i< getkHash(); i++){
			if(this.table[gerHash(name)] == 0)
				return false;		
		}
		return true;
	}
	public void add(String name)
        {
	    
	    name.hashCode()
	    for(int i = 0; i< getkHash(); i++){
	        int pname = gerHash(name, i);
		this.table[pname] = 1;
	    }
        } 
       
}
 

