//mpic++ -L/home/luis/mrmpi-7Apr14/src -I/home/luis/mrmpi-7Apr14/src -O3 -g -Wall -o fn fn.cpp -lmrmpi

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>
#include "mapreduce.h"
#include "keyvalue.h"

using namespace MAPREDUCE_NS;

void Usage(char* prog_name);
void Get_args(int argc, char* argv[],int* start, int* end);
int gcd(int u, int v);
void printArray(int* array, int size);
void generate(int itask, KeyValue *kv, void* ptr);
void my_reduce(char *key, int keybytes, char *multivalue, int nvalues, int *valuebytes, KeyValue *kv, void *ptr);
void output(char *key, int keybytes, char *multivalue, int nvalues, int *valuebytes, KeyValue *kv, void *ptr);

typedef struct V {
    int num;
    int den;
}Key;

typedef struct in {
    int start;
    int end;
    int* the_num;
    int* num;
    int* den;
}Input;

void printArray(int* array, int size)
{
    int i;
    for(i=0;i<size;i++)
        printf(" %d ", array[i]);
    printf("\n");
}

void Usage(char* prog_name) 
{
   fprintf(stderr, "usage:  %s <start> <end>\n", prog_name);
}


void Get_args(int argc, char* argv[],int* start, int* end) 
{
   if (argc != 3 ) 
   {
      Usage(argv[0]);
      exit(0);
   }
   *start = strtol(argv[1], NULL, 10);
   *end = strtol(argv[2],NULL,10);
}

int gcd(int u, int v)
{
    if (v == 0) 
        return u;
    return gcd(v, u % v);
}


void generate(int itask, KeyValue *kv, void* ptr)
{
    Input *inp = (Input*) ptr;
    int last = inp->end-inp->start+1;
    int i, j, factor, ii, sum, done, n;
    for (i = inp->start; i <= inp->end; i++) 
    {
        ii = i - inp->start;
        sum = 1 + i;
        inp->the_num[ii] = i;
        done = i;
        factor = 2;
        while (factor < done) 
        {
            if ((i % factor) == 0) 
            {
                sum += (factor + (i/factor));
                if ((done = i/factor) == factor)
                    sum -= factor;
            }
            factor++;
        }
        inp->num[ii] = sum;
        inp->den[ii] = i;
        n = gcd(inp->num[ii], inp->den[ii]);
        inp->num[ii] /= n;
        inp->den[ii] /= n;
        Key k;
        k.num = inp->num[ii];
        k.den = inp->den[ii];
        int n = inp->the_num[ii];
        kv->add((char *) &k,sizeof(Key),(char*) &n,sizeof(int));
    }
}

void my_reduce(char *key, int keybytes, char *multivalue, int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
    kv->add(key,keybytes,NULL,0);
}

void transform(int* numbers,char* mv, int nv,int* vb)
{
    int* values = (int*) mv;
    for (int i = 0; i < nv; ++i)
    {
        numbers[i] = values[i];
    }
}

void output(char *key, int keybytes, char *multivalue, int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
    Input* inp = (Input *) ptr;
    Key *k = (Key*) key;
    int* numbers=(int*)malloc(sizeof(int)*nvalues);
    
    transform(numbers,multivalue,nvalues,valuebytes);
    if(nvalues>1)
    {
        printf("The following numbers are friendly:\n");
        for (int i = 0; i < nvalues; ++i)
        {
            printf(" %d ", numbers[i]);
        }
        printf("\n");
    }
}


int main(int argc, char **argv)
{
    int start,end;
    Get_args(argc,argv,&start,&end);
    int rank,comm_size,size=end-start+1,offset;   

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&comm_size);
    
    offset = size/comm_size;
    if(rank == (comm_size-1))
        offset+=size%comm_size;
    
    int aux = size/comm_size;
    int my_start = rank*aux + start;
    int my_end = my_start + offset - 1;


    Input inp;
    
    inp.start = my_start;
    inp.end = my_end;
    
    

    int* the_num = (int*)malloc(sizeof(int)*offset);
    int* num = (int*)malloc(sizeof(int)*offset);
    int* den = (int*)malloc(sizeof(int)*offset); 

    inp.the_num = the_num;
    inp.num = num;
    inp.den = den;

    
    MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
    mr->verbosity = 0;
    mr->timer = 1;

    MPI_Barrier(MPI_COMM_WORLD);
    double tstart = MPI_Wtime();

    int m = mr->map(comm_size,&generate,&inp);

    mr->collate(NULL);
    
    int nunique = mr->reduce(&output,&inp);    

    MPI_Barrier(MPI_COMM_WORLD);
    double tstop = MPI_Wtime();
    
    if(rank == 0)
        printf("%lf\n",tstop -tstart);
    
    delete mr;
    MPI_Finalize();
}
