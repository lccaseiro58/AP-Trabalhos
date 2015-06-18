#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>

int gcd(int u, int v)
{
    if (v == 0) 
        return u;
    return gcd(v, u % v);
}

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
}  /* Usage */



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


void Map(int start, int end, int* the_num, int* num, int* den)
{
    int last = end-start+1;
    int i, j, factor, ii, sum, done, n;
    for (i = start; i <= end; i++) 
    {
        ii = i - start;
        sum = 1 + i;
        the_num[ii] = i;
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
        num[ii] = sum; den[ii] = i;
        n = gcd(num[ii], den[ii]);
        num[ii] /= n;
        den[ii] /= n;
    }
}


int main(int argc, char **argv)
{
    int start,end;
    Get_args(argc,argv,&start,&end);
    int rank,comm_size,size=end-start+1,offset;
    MPI_Init(&argc, &argv);

    double MPI_Wtime(void);
    double start_t, finish_t;
    start_t=MPI_Wtime();

    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&comm_size);

    offset = size/comm_size;
    if(rank == (comm_size-1))
        offset+=size%comm_size;
    
    int aux = size/comm_size;
    int my_start = rank*aux + start;
    int my_end = my_start + offset - 1;

    int *local_the_num = (int*)malloc(sizeof(int)*offset);
    int *local_num = (int*)malloc(sizeof(int)*offset);
    int *local_den = (int*)malloc(sizeof(int)*offset);

    int *the_num = (int*)malloc(sizeof(int)*size);
    int *num = (int*)malloc(sizeof(int)*size);
    int *den = (int*)malloc(sizeof(int)*size); 

    Map(my_start,my_end,local_the_num,local_num,local_den);

    MPI_Datatype elems;
    MPI_Type_contiguous(offset, MPI_INT, &elems);
    MPI_Type_commit(&elems);

    MPI_Allgather(local_the_num,1,elems,the_num,1,elems,MPI_COMM_WORLD);
    MPI_Allgather(local_num,1,elems,num,1,elems,MPI_COMM_WORLD);
    MPI_Allgather(local_den,1,elems,den,1,elems,MPI_COMM_WORLD);
   

    int i,j;
    if(rank == 0)
    {
        for (i = 0; i < size; i++) 
        {
            for (j = i+1; j< size; j++) 
            {
                if ((num[i] == num[j]) && (den[i] == den[j]))
                    printf ("%d and %d are FRIENDLY \n", the_num[i], the_num[j]);
            }
        }
    }
    finish_t=MPI_Wtime();
    
    if(rank == 0)
    {
        printf("tempo decorrido: %f\n", finish_t - start_t);
    }
    MPI_Finalize();
}