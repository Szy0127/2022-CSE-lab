#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  assert(id >= 0 && id < BLOCK_NUM);
  memcpy(buf,blocks[id],BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  // std::cout<<"write block "<<id<<std::endl;
  assert(id >= 0 && id < BLOCK_NUM);
  memcpy(blocks[id],buf,BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */


  //not include inode
  //inode free or not depends on type==0 instead of bitmap

  char buf[BLOCK_SIZE];

  blockid_t id_begin = IBLOCK(INODE_NUM,sb.nblocks)+1;
  blockid_t bitmap_id = BBLOCK(id_begin);
  blockid_t bitmap_max = BBLOCK(BLOCK_NUM);
  for(;bitmap_id < bitmap_max ;bitmap_id++){
    read_block(bitmap_id,buf);
    for(auto i = 0 ; i < BPB ;i++){
      // std::cout<<i<<std::endl;
      auto free_bit = buf[i/8] & (1<<(i%8));
      if(!free_bit){
        buf[i/8] |= (1<<(i%8));
        assert(bitmap_id >= 1 && bitmap_id < BLOCK_NUM);
        write_block(bitmap_id,buf);
        auto id = (bitmap_id-2) * BPB + i;
        // std::cout<<"alloc block:"<<id<<" "<<bitmap_id<<" "<<i<<std::endl;
        assert(id >= id_begin && id < BLOCK_NUM);
        bzero(buf,BLOCK_SIZE);  //must empty   indirect blocks id
        write_block(id,buf);
        return id;
      }
    }
  }


  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  // std::cout<<"free block:"<<id<<std::endl;
  auto id_begin = (IBLOCK(INODE_NUM,sb.nblocks)+1);
  assert(id >= id_begin && id < BLOCK_NUM);
  auto bitmap_id = BBLOCK(id);
  char buf[BLOCK_SIZE];
  assert(bitmap_id >= 1 && bitmap_id < BLOCK_NUM);
  read_block(bitmap_id,buf);
  auto i = id -(bitmap_id-2)*BPB;
  // std::cout<<id<<" "<<id_begin<<" "<<bitmap_id<<" "<<i<<std::endl;
  assert(i>=0 && i < BPB);
  buf[i/8] &= ~(1<<(i%8));
  write_block(bitmap_id,buf);
  // bzero(buf,BLOCK_SIZE);
  // write_block(id,buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  blockid_t block_id_begin = IBLOCK(INODE_NUM,sb.nblocks)+1;
  blockid_t bitmap_max = BBLOCK(block_id_begin) > BBLOCK(BLOCK_NUM) ? BBLOCK(BLOCK_NUM) : BBLOCK(block_id_begin);
  char buf[BLOCK_SIZE];
  bool finish = false;
  //memset is faster
  for(blockid_t bitmap_id = BBLOCK(0);bitmap_id <= bitmap_max ;bitmap_id++){
    if(finish){
      break;
    }
    read_block(bitmap_id,buf);
    for(auto i = 0 ; i < BPB ;i++){
        auto id = (bitmap_id-2) * BPB + i;
        if(id < block_id_begin){
          buf[i/8] |= (1<<(i%8));
        }else{
          finish = true;
          break;
        }
    }
    write_block(bitmap_id,buf);
  }
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()//:inum_available(1)
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}



/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  // not really malloc, just found a free block(inode) from blocks
  //search from first  
  assert(type!=0);
  char buf[BLOCK_SIZE];
  int i;
  inode_t *ino = nullptr;

  for(i = 1; i <= INODE_NUM ;i++){
    bm->read_block(IBLOCK(i,bm->sb.nblocks),buf);
    ino = (inode_t*)buf + i%IPB;
    if(ino->type==0){//free
      break;
    }
  }
  bzero(ino,sizeof(inode_t));
  for(auto i = 0 ; i <= NDIRECT;i++){
    assert(ino->blocks[i]==0);
  }
  ino->type = type;
  auto t = (decltype(ino->atime))time(nullptr);
  ino->atime = t; //access
  ino->mtime = t; //modify  write content
  ino->ctime = t; //change  metadata of inode
  put_inode(i,ino);
  return i;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  auto ino = get_inode(inum);
  assert(ino);

  bzero(ino,sizeof(inode_t));
  put_inode(inum,ino);
  
  delete ino;
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  // std::cout<<malloc(1)<<std::endl;
  // delete new char();
  assert(inum >= 1 && inum <= INODE_NUM);
  struct inode *ino;
  /* 
   * your code goes here.
   */
  char buf[BLOCK_SIZE];
  bm->read_block(IBLOCK(inum,bm->sb.nblocks),buf);
  ino = (struct inode*)buf + inum%IPB;

  //if return ino  ino->data is in local buf
  auto ino_ret = new inode_t();
  *ino_ret = *ino;
  return ino_ret;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  assert(ino);
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  // printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;


  ino->ctime = (decltype(ino->ctime))time(nullptr);

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);

}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  auto ino = get_inode(inum);
  assert(ino);
  *buf_out = (char*)malloc(ino->size);//free in extent_server.cc
  *size = ino->size;
  // std::cout<<inum<<" "<<*size<<std::endl;

  // we can only copy entire BLOCKSIZE if use read_block
  // but size of buf_out is not n * BLOCKSIZE ,which may cause malloc error several steps later, which is really hard to debug
  char buf[BLOCK_SIZE];

  for(unsigned int i = 0 ; i * BLOCK_SIZE < ino->size ;i++){
    auto block_id = ino->blocks[i];
    // bm->read_block(block_id,*buf_out+i*BLOCK_SIZE);
    if(i < NDIRECT){
      if((i+1)*BLOCK_SIZE < ino->size){
        bm->read_block(block_id,*buf_out+i*BLOCK_SIZE);
      }else{
        bm->read_block(block_id,buf);
        memcpy(*buf_out+i*BLOCK_SIZE,buf,ino->size - i*BLOCK_SIZE);
      }
    }else{
      blockid_t indirect_blocks[BLOCK_SIZE/sizeof(blockid_t)];
      bm->read_block(block_id,(char*)indirect_blocks);
      for(; i * BLOCK_SIZE < ino->size ;i++){
        block_id = indirect_blocks[i-NDIRECT];
        if((i+1)*BLOCK_SIZE < ino->size){
          bm->read_block(block_id,*buf_out+i*BLOCK_SIZE);
        }else{
          bm->read_block(block_id,buf);
          memcpy(*buf_out+i*BLOCK_SIZE,buf,ino->size - i*BLOCK_SIZE);
        }
      }
    }
    // std::cout<<"read inode:"<<inum<<" block:"<<block_id<<" content:"<<*buf_out<<std::endl;
  }

  ino->atime = (decltype(ino->atime))time(nullptr);
  put_inode(inum,ino);
  delete ino;

  
  
  return;
}

void check_indirect_blocks(block_manager *bm,blockid_t  id){
  if(id==0){
    return;
  }
  blockid_t indirect_blocks[BLOCK_SIZE/sizeof(blockid_t)];
  bm->read_block(id,(char*)indirect_blocks);
  // std::cout<<"[check]read indirect"<<id<<std::endl;
  for(unsigned i = 0 ;i < BLOCK_SIZE/sizeof(blockid_t) ;i++){
    assert(indirect_blocks[i] >=0 && indirect_blocks[i] < BLOCK_NUM);
  }
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */

  // inum(inode) <--> file <--> buf
  // inode.blocks --> blocks

  auto ino = get_inode(inum);
  // std::cout<<"write"<<inum<<" "<<size<<" "<<ino->size<<" "<<ino->blocks[0]<<std::endl;
  assert(ino);
  // std::cout<<"check"<<inum;
  // check_indirect_blocks(bm,ino->blocks[NDIRECT]);
  assert(size>=0);
  // auto old_size = ino->size;
  ino->size = size;
  int i;
  for(i = 0 ;i * BLOCK_SIZE < size ;i++){
    auto block_id = ino->blocks[i];
    // std::cout<<"inum:"<<inum<<" block["<<i<<"]="<<block_id<<std::endl;
    if(block_id == 0){
      block_id = bm->alloc_block();
      ino->blocks[i] = block_id;
      // if(i == NDIRECT){
        // std::cout<<"alloc indirect"<<inum<<" "<<block_id<<std::endl;
        // check_indirect_blocks(bm,block_id);
      // }
    }

    if(i < NDIRECT){
      if((i+1) * BLOCK_SIZE < size){
        bm->write_block(block_id,buf+i*BLOCK_SIZE);
      }else{
        char _buf[BLOCK_SIZE];
        bzero(_buf,'\0');
        memcpy(_buf,buf + i * BLOCK_SIZE,size - i * BLOCK_SIZE);
        bm->write_block(block_id,_buf); // 1/15 segmentation fault
      }
    }else{
      assert(i==NDIRECT);
      // std::cout<<"322read indirect"<<inum<<" "<<block_id<<std::endl;
      // check_indirect_blocks(bm,block_id);
      blockid_t indirect_blocks[BLOCK_SIZE/sizeof(blockid_t)];
      bm->read_block(block_id,(char*)indirect_blocks);
      for(;i * BLOCK_SIZE < size;i++){
        assert(i < (int)(NDIRECT+NINDIRECT));
        block_id = indirect_blocks[i-NDIRECT];
        if(block_id == 0){
          block_id = bm->alloc_block();
          indirect_blocks[i-NDIRECT] = block_id;
        }
        // std::cout<<block_id<<std::endl;
        assert(block_id >= 0 && block_id < BLOCK_NUM);
        if((i+1) * BLOCK_SIZE < size){
          bm->write_block(block_id,buf+i*BLOCK_SIZE);
        }else{
          char _buf[BLOCK_SIZE];
          bzero(_buf,'\0');
          memcpy(_buf,buf+ i * BLOCK_SIZE,size - i * BLOCK_SIZE);
          bm->write_block(block_id,_buf); // 1/15 segmentation fault
        }
      }
      assert(ino->blocks[NDIRECT] > 0 && ino->blocks[NDIRECT] < BLOCK_NUM);
      // std::cout<<"336write indirect"<<inum<<" "<<ino->blocks[NDIRECT]<<std::endl;
      // check_indirect_blocks(bm,ino->blocks[NDIRECT]);
      bm->write_block(ino->blocks[NDIRECT],(char*)indirect_blocks);
      // check_indirect_blocks(bm,ino->blocks[NDIRECT]);
      break;
    }
    // std::cout<<"write inode:"<<inum<<" block:"<<block_id<<std::endl;//" content:"<<(buf+i*BLOCK_SIZE)<<std::endl;
  }

  //free useless blocks
  for(; i < (int)NDIRECT ; i++){
    auto block_id = ino->blocks[i];
    if(block_id > 0){
      assert(block_id >=0 && block_id < BLOCK_NUM);
      bm->free_block(block_id);
      ino->blocks[i] = 0;
    }else{
      break;
    }
  }
  
  if(ino->blocks[NDIRECT]>0){
      assert(i>=NDIRECT);
      // std::cout<<"361read indirect"<<inum<<" "<<ino->blocks[NDIRECT]<<std::endl;
      // check_indirect_blocks(bm,ino->blocks[NDIRECT]);
      blockid_t indirect_blocks[BLOCK_SIZE/sizeof(blockid_t)];
      bm->read_block(ino->blocks[NDIRECT],(char*)indirect_blocks);
      for(;i < (int)(NDIRECT + NINDIRECT);i++){
          auto block_id = indirect_blocks[i-NDIRECT];
          if(block_id > 0){
            // std::cout<<block_id<<std::endl;
            assert(block_id >=0 && block_id < BLOCK_NUM);
            bm->free_block(block_id);
            indirect_blocks[i-NDIRECT] = 0;
          }else{
            break;
          }
      }
      if(indirect_blocks[0]==0){
        bm->free_block(ino->blocks[NDIRECT]);
        ino->blocks[NDIRECT] = 0;
      }else{
        assert(ino->blocks[NDIRECT] >0 && ino->blocks[NDIRECT] < BLOCK_NUM);
        // std::cout<<"379write indirect"<<inum<<" "<<ino->blocks[NDIRECT]<<std::endl;
        // check_indirect_blocks(bm,ino->blocks[NDIRECT]);
        bm->write_block(ino->blocks[NDIRECT],(char*)indirect_blocks);
        // check_indirect_blocks(bm,ino->blocks[NDIRECT]);
      }
  }

  auto t =  (decltype(ino->atime))time(nullptr);
  ino->atime = t;
  ino->mtime = t;
  put_inode(inum,ino);
  // std::cout<<"put inode:"<<inum<<" size:"<<size<<std::endl;
  delete ino;
  return;
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  auto ino = get_inode(inum);
  assert(ino);
  a.atime = ino->atime;
  a.ctime = ino->ctime;
  a.mtime = ino->mtime;
  a.size = ino->size;
  a.type = ino->type;
  
  delete ino;
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  auto ino = get_inode(inum);
  assert(ino);
  for(unsigned int i = 0 ;i * BLOCK_SIZE < ino->size ;i++){
    auto block_id = ino->blocks[i];
    if(block_id==0){
      break;
    }
    if(i < NDIRECT){
      bm->free_block(block_id);
    }else{
      blockid_t indirect_blocks[BLOCK_SIZE/sizeof(blockid_t)];
      bm->read_block(block_id,(char*)indirect_blocks);
      // std::cout<<"419 read indirect block"<<block_id<<std::endl;
      for(;i * BLOCK_SIZE < ino->size;i++){
        block_id = indirect_blocks[i-NDIRECT];
        bm->free_block(block_id);
      }
      // std::cout<<"free indirect block"<<block_id<<std::endl;
      bm->free_block(ino->blocks[NDIRECT]);
    }
  }
  delete ino;
  free_inode(inum);
  
  return;
}
