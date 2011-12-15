/* vmemraid skeleton */
/* vmemraid_main.c */
/* by William A. Katsak <wkatsak@cs.rutgers.edu> */
/* for CS 416, Fall 2011, Rutgers University */

/* This sets up some functions for printing output, and tagging the output */
/* with the name of the module */
/* These functions are pr_info(), pr_warn(), pr_err(), etc. and behave */
/* just like printf(). You can search LXR for information on these functions */
/* and other pr_ functions. */
#define pr_fmt(fmt) KBUILD_MODNAME ": " fmt

#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/module.h>
#include <linux/fs.h>
#include <linux/blkdev.h>
#include <linux/spinlock.h>
#include <linux/errno.h>
#include <linux/hdreg.h>
#include "vmemraid.h"

static int major_num = 0;


/* Pointer for device struct. You should populate this in init */
struct vmemraid_dev *dev;

/* They call this XOR, store answer in buffer */
void xor(char* input1, char* input2, char* buffer) {
	int i, size=VMEMRAID_HW_SECTOR_SIZE/sizeof(unsigned int);

	unsigned int *ubuffer = (unsigned int *) buffer;
	unsigned int *uinput1 = (unsigned int *) input1;
	unsigned int *uinput2 = (unsigned int *) input2;

	for (i = 0; i < size; i++) {
		/* They call me pointer arithmetic ~Gerard */
		/* They call you syntactic sugar ~Jerry */
		ubuffer[i] = uinput1[i] ^ uinput2[i];
	}
}

/* Should take a list of blocks and store the xor of those blocks
 * into buffer */
void xor_all_the_blocks(block_list bl, char* buffer) {
	int i;
	for(i = 0; i < (bl->num_elements - 1); i++) {
		xor(bl->blocks[i], bl->blocks[i+1], buffer);
	}
}

/* Takes a bit flags of dropped disks, check if the disk is dropped. */
int is_dropped_disk(unsigned int disk_drop_flag, unsigned int disk) {
	unsigned int shift_bits = 1 << disk;
	return ((shift_bits & disk_drop_flag) >> disk == 1) ? 1 : 0;
}

/* Hold list of pointers to blocks */
int add_to_bl(block_list bl, char* buffer) {
	int success = 0;
	if (bl->num_elements < NUM_DISKS) {
		bl->blocks[(bl->num_elements)++] = buffer;
		success = 1;
	}
	return success;
}

/* Read physical_block and put that block into buffer, works even if 1 disk dropped */
static void vmemraid_raid5_read(struct vmemraid_dev *dev, unsigned long physical_block,
		char* buffer) {

	int i;

	/* On which disk the physical block lies on */
	unsigned block_disk_loc = physical_block % NUM_DISKS;
	
	/* On which row the physical block lies on in the disk */
	unsigned row_loc = physical_block / NUM_DISKS;

	pr_info("Starting raid5 read");
	
	pr_info("   physical_block: %lu, row_loc: %u, block_disk_loc:  %u\n",
			physical_block, row_loc, block_disk_loc);

	if(!is_dropped_disk(dev->disks_dropped_bits, block_disk_loc)) {
		memdisk_read_block(dev->disk_array->disks[block_disk_loc], buffer, row_loc);
		pr_info("Finished raid5 read, no disks dropped");
		return;
	}
	
	dev->bl->num_elements = 0;

	/* This disk is dropped, we gotta do parity calcs */
	for(i = 0; i < NUM_DISKS; i++) {
		if(i == block_disk_loc)
			continue;
		
		memdisk_read_block(dev->disk_array->disks[i], dev->bufferlist[i], row_loc);
		if(!add_to_bl(dev->bl, dev->bufferlist[i])) {
			pr_err("Overflowing Buffer");
		}
	}
	/* XOR and recreate broken block */
	xor_all_the_blocks(dev->bl, buffer);

	pr_info("Finished raid5 read");
}

static void vmemraid_raid5_write(struct vmemraid_dev *dev, unsigned long physical_block,
		char* buffer) {
	
	int i;

	/* On which disk the physical block lies on */
	unsigned block_disk_loc = physical_block % NUM_DISKS;
	
	/* On which row the physical block lies on in the disk */
	unsigned row_loc = physical_block / NUM_DISKS;

	/* On which disk the parity lies on */
	unsigned parity_disk_loc = row_loc % (NUM_DISKS);

	/* Do I need this? 
	char* orig_block = dev->bufferlist[block_disk_loc];
	vmemraid_raid5_read(dev, hw_block, block_disk_loc, parity_loc, orig_block);
	*/
	
	pr_info("Entered raid5 write");

	pr_info("   physical_block: %lu, row_loc: %u, parity_disk_loc: %u, block_disk_loc:  %u\n",
			physical_block, row_loc, parity_disk_loc, block_disk_loc);

	/* If current block we are writing to isn't dropped, then we can write our 
	 * changes now. If it is, don't write as its pointless/shouldn't work.*/
	if(!is_dropped_disk(dev->disks_dropped_bits, block_disk_loc)) {
		memdisk_write_block(dev->disk_array->disks[block_disk_loc], buffer, row_loc);
	}

	/* If Parity is gone, we don't have to update it, so can just return */
	if(is_dropped_disk(dev->disks_dropped_bits, parity_disk_loc)) {
		return;
	}

/* Updating Parity follows */

	dev->bl->num_elements = 0;

	/* Add the new written block */
	if(!add_to_bl(dev->bl, buffer)) {
		pr_err("Overflowing Bufferlist");
	}

	for(i = 0; i < NUM_DISKS; i++) {
		unsigned long phys_block;
		if(i == parity_disk_loc || block_disk_loc == i)
			continue;
	
		/* Physical block location of each block in row */
		phys_block = physical_block - block_disk_loc + i;
		vmemraid_raid5_read(dev, phys_block, dev->bufferlist[i]);

		if(!add_to_bl(dev->bl, dev->bufferlist[i])) {
			pr_err("Overflowing Bufferlist");
		}
	}

	/* Re-create parity and write it to disk */
	xor_all_the_blocks(dev->bl, dev->bufferlist[parity_disk_loc]);
	memdisk_write_block(dev->disk_array->disks[parity_disk_loc],
			dev->bufferlist[parity_disk_loc], row_loc);

/* Done Updating Parity */

	pr_info("Finished raid5 write");

}


/*
 * Handle an I/O request.
 * 
 * write == 1 means write. 
 */
static void vmemraid_transfer(struct vmemraid_dev *dev, sector_t sector,
		unsigned long nsect, char *buffer, int write) {

	unsigned long i = 0, nbytes;
	unsigned int ratio;
	
	/* How many kernel blocks we can fit in hw blocks */
	ratio = VMEMRAID_HW_SECTOR_SIZE/KERNEL_SECTOR_SIZE;
	
	/* How many bytes do we have to read */
	nbytes = nsect * KERNEL_SECTOR_SIZE;

	pr_info("sector, nsect: (%ld, %lu)", sector, nsect);

	/* More than 1 disk dropped, this transfer shouldn't work */
	if(dev->num_dropped_disks > 1) {
		pr_info("More than 1 disk dropped, we aren't transferring");
		return;
	}

	if (((sector * KERNEL_SECTOR_SIZE) + nbytes) > dev->size) {
		pr_warn("Beyond-end write (%ld %ld)\n", (unsigned long)sector, nbytes);
		return;
	}

	/* Read the number of sectors one at a time */
	for(i = 0; i < nsect; i++) {

		/* Which kernel block we want */
		unsigned long kernel_block = sector + i;
		/* The hw block where sector is located.
		 * Only data counts as blocks in this model, not parity */
		unsigned long hw_block = kernel_block/ratio;
		/* Which sub-block in large block does caller want */
		unsigned long offset = (kernel_block % ratio) * KERNEL_SECTOR_SIZE;

		/* How many parity blocks we passed for each block,	
		 * this is used to compute the location on what disk each
		 * block is on the disk. Our parities start from left to right. */
		unsigned long parities_passed = (hw_block/ NUM_DISKS) + 1;
		/* What block this hw_block lies on in the hardware.
		 * Parities count as blocks in this model */
		unsigned long physical_block = hw_block + parities_passed;

		char* block_buffer;

//		static char finalbuff[VMEMRAID_HW_SECTOR_SIZE];

		pr_info("kernel_block: %lu, hw_block: %lu, offset: %lu, physical_block: %lu, nsect: %lu", 
			kernel_block, hw_block, offset, physical_block, nsect);

// Don't Need?		
//		/* On which disk the parity block lies for current row */
//		unsigned int parity_disk_loc = (hw_block / (NUM_DISKS - 1)) % NUM_DISKS;
// End Don't Need?		

		/* What block we are looking at */
		block_buffer = dev->bufferlist[NUM_DISKS];
//		char* block_buffer = finalbuff; //LINE ABOVE is ORIG
		vmemraid_raid5_read(dev, physical_block, block_buffer);

		if (write) {
			memcpy(block_buffer + offset, buffer + (i*KERNEL_SECTOR_SIZE), KERNEL_SECTOR_SIZE);
			vmemraid_raid5_write(dev, physical_block, block_buffer);
		}
		else {
			memcpy(buffer + (i*KERNEL_SECTOR_SIZE), block_buffer + offset, KERNEL_SECTOR_SIZE);
		}
	}

}

/* Request function. This is registered with the block API and gets */
/* called whenever someone wants data from your device */
/* NOTE: This function is atomic! It might self-destruct! */
static void vmemraid_request(struct request_queue *q)
{
	struct request *req;

	/* Start the next request (if possible), then return it */
	req = blk_fetch_request(q);
	while (req != NULL) {

		/* Not a filesystem request or no request */
		if (req == NULL || (req->cmd_type != REQ_TYPE_FS)) {
			pr_info("Skip non-CMD request\n");
			__blk_end_request_all(req, -EIO);
			continue;
		}
		
		vmemraid_transfer(dev, blk_rq_pos(req), blk_rq_cur_sectors(req),
				req->buffer, rq_data_dir(req));

		if ( ! __blk_end_request_cur(req, 0) ) {
			req = blk_fetch_request(q);
		}
	}
}

/* Open function. Gets called when the device file is opened */
static int vmemraid_open(struct block_device *block_device, fmode_t mode)
{
	struct vmemraid_dev *dev = block_device->bd_inode->i_bdev->bd_disk->private_data;
	pr_info("Opened vmemraid");
	spin_lock(&dev->lock);
	if (! dev->users)
		check_disk_change(block_device->bd_inode->i_bdev);
	dev->users++;
	spin_unlock(&dev->lock);
	return 0;
}

/* Release function. Gets called when the device file is closed */
static int vmemraid_release(struct gendisk *gd, fmode_t mode)
{
	struct vmemraid_dev *dev = gd->private_data;
	pr_info("Released vmemraid");
	spin_lock(&dev->lock);
	dev->users--;
	spin_unlock(&dev->lock);
	return 0;
}

/* getgeo function. Provides device "geometry". This should be */
/* the old cylinders:heads:sectors type geometry. As long as you */
/* populate dev->size with the total usable bytes of your disk */
/* this implentation will work */
int vmemraid_getgeo(struct block_device *block_device, struct hd_geometry *geo)
{
	long size;

	size = dev->size / KERNEL_SECTOR_SIZE;
	geo->cylinders = (size & ~0x3f) >> 6;
	geo->heads = 4;
	geo->sectors = 16;
	geo->start = 0;

	return 0;
}

/* This gets called when a disk is dropped from the array */
/* NOTE: This will be called with dev->lock HELD */
int vmemraid_callback_drop_disk(int disk_num)
{
	if (!is_dropped_disk(dev->disks_dropped_bits, disk_num)) {
		dev->disks_dropped_bits |= (1 << disk_num);
		(dev->num_dropped_disks)++;
	}
	return 1;
}
/* This gets called when a dropped disk is replaced with a new one */
/* NOTE: This will be called with dev->lock HELD */
int vmemraid_callback_new_disk(int disk_num)
{
	/* Re-enable that drive */
	if(is_dropped_disk(dev->disks_dropped_bits, disk_num)) {
		dev->disks_dropped_bits ^= (1 << disk_num);
		(dev->num_dropped_disks)--;
	}
	else {
		return 1;
	}

	/* No more dropped disks, should re-initialize broken values */
	if(dev->num_dropped_disks < 1) {
		int i, num_blocks;
		num_blocks = memdisk_num_blocks(dev->disk_array->disks[disk_num]);

		for (i = 0; i < num_blocks; i++) {
			unsigned long phys_block = disk_num + (i * NUM_DISKS);

			vmemraid_raid5_read(dev, phys_block, dev->bufferlist[NUM_DISKS]);
			memdisk_write_block(dev->disk_array->disks[disk_num], 
					dev->bufferlist[NUM_DISKS], phys_block);
		}
	}
	return 1;
}

/*
 * This structure must be passed the the block driver API when the
 * device is registered
 */
static struct block_device_operations vmemraid_ops = {
	.owner			= THIS_MODULE,
	.open			= vmemraid_open,
	.release		= vmemraid_release,
	.getgeo			= vmemraid_getgeo,
	/* do not tamper with or attempt to replace this entry for ioctl */
	.ioctl		= vmemraid_ioctl
};

/* Init function */
/* This is executed when the module is loaded. Should result in a usable */
/* driver that is registered with the system */
/* NOTE: This is where you should allocate the disk array */
static int __init vmemraid_init(void)
{
	int i, size;

	/* Set up our internal device. */
	dev = (struct vmemraid_dev*)kmalloc(sizeof(struct vmemraid_dev), GFP_KERNEL);
	if (!dev)
		goto out_dev;
	dev->size = DISK_SIZE_SECTORS * KERNEL_SECTOR_SIZE * (NUM_DISKS-1);
	spin_lock_init(&dev->lock);

	pr_info("Dev struct created");

	/* Set up flags for dropped disks */
	dev->disks_dropped_bits = 0;
	dev->num_dropped_disks = 0;

	/* Allocate space for blocklist */

	dev->bl = (block_list)kmalloc(sizeof(struct block_list_), GFP_KERNEL);
	if (!dev->bl)
		goto out_disk_array;
	dev->bl->num_elements = 0;


	pr_info("Block list created");

	/* Allocate space for bufferlist */

	size = sizeof(dev->bufferlist) / sizeof(char*);
	for(i = 0; i < size; i++) {
		dev->bufferlist[i] = (char *)kmalloc(sizeof(char) * VMEMRAID_HW_SECTOR_SIZE, GFP_KERNEL);
		if(!dev->bufferlist[i])
			goto out_buff_list;
	}

	pr_info("Bufferlist created");

	/* Create disk array */

	dev->disk_array = create_disk_array(NUM_DISKS, DISK_SIZE_SECTORS * 
		KERNEL_SECTOR_SIZE);
	if(!(dev->disk_array)) 
		goto out_buff_list;


	pr_info("Disk array created");

	/* Get a request queue. */
	dev->queue = blk_init_queue(vmemraid_request, &dev->lock);
	if (dev->queue == NULL)
		goto out_queue;
	blk_queue_logical_block_size(dev->queue, KERNEL_SECTOR_SIZE);

	/*
	 * register_blkdev() registers the block device with the kernel.
	 * Pass in the major number your device will be using, and
	 * the associated name of the module. If major is passed as 0, the
	 * kernel allocates a new major number and returns it to the caller.
	 * If a negative value is returned, then an error has occured.
	 */
	major_num = register_blkdev(0, "vmemraid");
	if (major_num <= 0) {
		pr_err("Unable to get major number\n");
		goto out_blkdev;
	}

	/* And the gendisk structure. */

	/* Allocate memory for the disk. */
	dev->gd = alloc_disk(VMEMRAID_NUM_MINORS);
	if (!dev->gd)
		goto out_unregister;

	dev->gd->major = major_num;
	/* The first minor device number corresponding to this disk. */
	dev->gd->first_minor = 0;
	/* Pointer to a block_device_operations struct */
	dev->gd->fops = &vmemraid_ops;

	/* Pointer to internal data. */
	dev->gd->private_data = dev;

	strcpy(dev->gd->disk_name, "vmemraid");

	set_capacity(dev->gd, dev->size/KERNEL_SECTOR_SIZE);

	dev->gd->queue = dev->queue;

	pr_info("Successfully initialized disk");

	/* Makes the disk available to the system and makes it live!*/
	add_disk(dev->gd);

	return 0;

/* Gotta free up all the stuff we allocated, we have no memory! */
out_unregister:
	/* Unregister the device. MAKE SURE that the arguments
	 * are identical to those passed into register_blkdev() */
	unregister_blkdev(major_num, "vmemraid");
out_blkdev:
	blk_cleanup_queue(dev->queue);	
	
out_queue:
	destroy_disk_array(dev->disk_array);
out_buff_list:
	while(i > 1) {
		kfree(dev->bufferlist[i-1]);
		i++;
	}
	kfree(dev->bl);
out_disk_array:	
	kfree(dev);
out_dev: 
	return -ENOMEM;
}

/* Exit function */
/* This is executed when the module is unloaded. This must free any and all */
/* memory that is allocated inside the module and unregister the device from */
/* the system. */
static void __exit vmemraid_exit(void)
{
	int size, i;
	del_gendisk(dev->gd);
	unregister_blkdev(major_num, "vmemraid");
	blk_cleanup_queue(dev->queue);
	destroy_disk_array(dev->disk_array);
	size = sizeof(dev->bufferlist) / sizeof(char*);
	for(i = 0; i < size; i++) {
		kfree(dev->bufferlist[i]);
	}
	kfree(dev->bl);
	kfree(dev);
}

/* Tell the module system where the init and exit points are. */
module_init(vmemraid_init);
module_exit(vmemraid_exit);

/* Declare the license for the module to be GPL. This can be important in some */
/* cases, as certain kernel functions are only exported to GPL modules. */
MODULE_LICENSE("GPL");

