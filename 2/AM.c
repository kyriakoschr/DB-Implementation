#include "BF.h"
#include "AM.h"

OpenFile OpenFiles[MAXOPENFILES];
OpenScan OpenScans[MAXSCANS];

void AM_Init( void ){
	OpenFile tempfile;
	OpenScan tempscan;
	int i;
	tempfile.fileDesc=EMPTY;
	tempfile.recsize=0;
	strcpy(tempfile.FileName,"EMPTY");
	tempscan.FirstBlock=NULL;
	tempscan.LastBlock=NULL;
	tempscan.op=0;
	tempscan.value=NULL;
	for (i=0;i<MAXOPENFILES;i++)
		memcpy(&(OpenFiles[i]),&tempfile,sizeof(OpenFile));
	for (i=0;i<MAXSCANS;i++)
		memcpy(&(OpenScans[i]),&tempscan,sizeof(OpenScan));
}

int AM_CreateIndex(char *fileName,char attrType1,int attrLength1,char attrType2,int attrLength2){
	void *ptr;
	FileHead headpoint;
	int fileDesc;
	BF_Init();
	headpoint.title="This is the first block\n";
	headpoint.depth=0;
	headpoint.DataQuantity=(BLOCK_SIZE-sizeof(BlockHead))/(sizeof(DataBlockUnit));
	headpoint.IndexQuantity=(BLOCK_SIZE-sizeof(BlockHead)/(sizeof(IndexBlockUnit)));
	headpoint.sizev1=attrLength1;
	headpoint.sizev2=attrLength2;
	headpoint.v1=attrType1;
	headpoint.v2=attrType2;
	if(BF_CreateFile(fileName)==0){
        fileDesc=BF_OpenFile(fileName);
        if(fileDesc>-1)
            if(BF_AllocateBlock(fileDesc)==0)
                if(BF_ReadBlock(fileDesc,0,(void**)&ptr)==0){
					memcpy(ptr,&headpoint,sizeof(FileHead));
					if(BF_WriteBlock(fileDesc,0)==0)
                        if(BF_CloseFile(fileDesc)==0)
                            return AME_OK;
						else BF_PrintError("BF_CloseFile in Create Index error\n");
                    else BF_PrintError("BF_WriteBlock in Create Index error\n");}
                else BF_PrintError("BF_ReadBlock in Create Index error\n");
            else BF_PrintError("BF_AllocateBlock in Create Index error\n");
        else BF_PrintError("BF_OpenFile in Create Index error\n");}
    else BF_PrintError("BF_CreateFile in Create Index error\n");
	return AME_ERR;
}

int AM_OpenIndex (char *fileName ){
	int i;
	OpenFile temp;
	FileHead temphead;
	void* ptr;
	temp.fileDesc=BF_OpenFile(fileName);
	if (temp.fileDesc<0){
		BF_PrintError("Sfalma stin BF_OpenFile tis AM_OpnIndex\n");
		return AME_ERR;
		}
	else{
		if(BF_ReadBlock(temp.fileDesc,0,(void**)&ptr)==0){
			memcpy(&temphead,ptr,sizeof(FileHead));
			if(strcmp(temphead.title,"This is the first block\n")==0){
				strcpy(temp.FileName,fileName);
				for(i=0;i<MAXOPENFILES;i++)
					if (OpenFiles[i].fileDesc==EMPTY){
						memcpy(&(OpenFiles[i]),&temp,sizeof(OpenFile));
						return i;
					}
				printf("Den iparxei xwros gia na annixei to arxeio\n");
				return AME_ERR;}
			}

		else
			BF_PrintError("Sfalma stin BF_ReadFile tis AM_OpnIndex\n");}
	return AME_ERR;
}


int AM_InsertEntry(int fileDesc,void *value1,void *value2){
	void *dataptr;
	void *indexptr;
	int temp;
	BlockHead tempIndHead;
	BlockHead tempDataHead;
	IndexBlockUnit tempIndUnit;
	DataBlockUnit tempDataUnit;
	FileHead tempheadpoint;
	if(BF_ReadBlock(fileDesc,0,(void**)&indexptr)!=0){
		BF_PrintError("Sfalma stin BF_ReadBlock tis AM_InsertEntry\n");
		return AME_ERR;}
	memcpy(indexptr,&tempheadpoint,sizeof(FileHead));
	if(BF_GetBlockCounter(fileDesc)==1){//den iparxei kapoia eggrafi sto arxeio epeidi den iparxei kapoio allo block ektos apo to head
		if(BF_AllocateBlock(fileDesc)!=0){//dimiourgia rizas
			BF_PrintError("Sfalma stin BF_AllocateBlock tis AM_InsertEntry\n");
			return AME_ERR;}
		tempIndHead.flag=ROOTBLOCK;
		tempIndHead.Counter=1;
		tempIndHead.MinVal=0;
		tempIndHead.Next=EMPTY;
		if(BF_ReadBlock(fileDesc,1,(void**)&indexptr)!=0){//pernw dixti sti riza
			BF_PrintError("Sfalma stin BF_ReadBlock tis AM_InsertEntry\n");
			return AME_ERR;}
		memcpy(indexptr,&tempIndHead,sizeof(BlockHead));	//grafw to index block head sto index block
		if(BF_AllocateBlock(fileDesc)!=0){//dimiourgia data block
			BF_PrintError("Sfalma stin BF_AllocateBlock tis AM_InsertEntry\n");
			return AME_ERR;}
		tempDataHead.Next=EMPTY;
		if(BF_ReadBlock(fileDesc,2,(void**)&dataptr)!=0){//pernw dixti sto data block
			BF_PrintError("Sfalma stin BF_ReadBlock tis AM_InsertEntry\n");
			return AME_ERR;}
		tempDataUnit.value1=value1;
		tempDataUnit.value2=value2;
		tempDataHead.MinVal=(long int)value1;
		tempDataHead.Counter=1;
		memcpy(dataptr,&tempDataHead,sizeof(BlockHead)); //grafw to data block head sto data block
		memcpy(dataptr+sizeof(BlockHead),&tempDataUnit,sizeof(DataBlockUnit)); //grafw to data unit sto data block
		tempIndUnit.KeyValue=(long int)value1;
		tempIndUnit.Pointer=2;
		temp=EMPTY;
		memcpy(indexptr+sizeof(BlockHead),&temp,sizeof(temp));
		memcpy(indexptr+sizeof(BlockHead)+sizeof(temp),&tempIndUnit,sizeof(IndexBlockUnit));
		if(BF_WriteBlock(fileDesc,1)!=0){//grafw ti riza
			BF_PrintError("Sfalma stin BF_WriteFile tis AM_InsertEntry\n");
			return AME_ERR;}
		if(BF_WriteBlock(fileDesc,2)!=0){//grafw to datablock
			BF_PrintError("Sfalma stin BF_WriteFile tis AM_InsertEntry\n");
			return AME_ERR;}
		return AME_OK;
	}//else if //den einai adeio to block pou thelw na grapsw mesa
}

/*
int AM_OpenIndexScan(int fileDesc,int op,void *value){
	int found=0;
	void *ptr;
	void *ptr2;
	int i;
	int temp;
	int eob=0;
	Head temphead;
	if(BF_ReadBlock(fileDesc,0,(void**)&ptr)==0){
		memcpy(&temphead,ptr,sizeof(Head)); // gia na parw to ipsos tou dendrou
		if(BF_ReadBlock(fileDesc,1,(void**)&ptr)==0){// gia na pame sto epomeno block (riza)
			for (i=0;i<temphead.depth;i++){
				while (found==0){
					memcpy(&temp,(ptr+8),sizeof(int));
					if (temp>(long int)value){
						found=1;}
					else
						ptr=ptr+8+sizeof(int);
				}
				if (found==0)

				ptr2=ptr;
				ptr=ptr2;
				found=0;
			}

		}
	}
	PrintError("Sfalma stin BF_ReadFile tis AM_OpnIndex\n");
	return AME_ERR;
}*/















