use crate::types::InodeDescriptor;
use crate::vfs::block::{BlockTopology, VirtualBlock};
use crate::ShmrFs;
use dbus::blocking::Connection;
use dbus::MethodErr;
use dbus_crossroads::{Context, Crossroads};
use log::{error, info};

pub fn dbus_server(shmr_fs: ShmrFs) -> Result<(), dbus::Error> {
    let c = Connection::new_session()?;
    c.request_name("co.volf.shmr", false, true, false)?;
    let mut cr = Crossroads::new();

    // possible signals?
    // - drive replaced?
    // - drive degraded?
    // - reload config
    // -

    let iface_token = cr.register("co.volf.shmr.ShmrFs1", |b| {
        // This row advertises (when introspected) that we can send a HelloHappened signal.
        // We use the single-tuple to say that we have one single argument, named "sender" of type "String".
        // The msg_fn returns a boxed function, which when called constructs the message to be emitted.
        // let hello_happened = b.signal::<(String,), _>("HelloHappened", ("sender",)).msg_fn();

        // Let's add a method to the interface. We have the method name, followed by
        // names of input and output arguments (used for introspection). The closure then controls
        // the types of these arguments. The last argument to the closure is a tuple of the input arguments.
        b.method("RewriteBlock", ("inode", "block_idx", "topology", "pool_name",), ("reply",), move |ctx: &mut Context, shmr: &mut ShmrFs, (inode, block_idx, topology, pool_name,): (u64, u64, String, String,)| {

            if !shmr.config.has_pool(&pool_name) {
                error!("dbus::{}]. pool '{}' does not exist", ctx.method(), &pool_name);
                return Err(MethodErr::invalid_arg("Invalid Pool Name"));
            }

            let new_block_topology = match BlockTopology::try_from(topology) {
                Ok(topology) => topology,
                Err(err) => {
                    error!("dbus::{}] unable to parse Topology. {:?}", ctx.method(), err);
                    return Err(MethodErr::invalid_arg("Unable to parse Topology"));
                }
            };

            // check if the inode exists, and is a RegularFile
            let superblock_entry = shmr.superblock.get_mut(&inode).unwrap();
            match superblock_entry {
                None => {
                    error!("dbus::{}] Inode {} does not exist", ctx.method(), &inode);
                    return Err(MethodErr::invalid_arg("Specified Inode does not exist"));
                }
                Some(mut entry) => match &mut entry.inode_descriptor {
                    InodeDescriptor::File(virtual_file) => {

                        // TODO There is too much nested logic here

                        // populate the old file, in case it hasn't been already
                        virtual_file.populate(shmr.config.clone());

                        let mut new_block = match VirtualBlock::create_with_pool(inode, block_idx, &shmr.config, virtual_file.block_size, new_block_topology, &pool_name) {
                            Ok(block) => block,
                            Err(err) => {
                                error!("dbus::{}] inode:{}] Failed to create VirtualBlock. {:?}", ctx.method(), &inode, err);
                                return Err(MethodErr::failed("Failed to create new VirtualBlock"));
                            }
                        };
                        new_block.populate(shmr.config.clone());

                        if let Err(e) = virtual_file.replace_block(block_idx as usize, new_block) {
                            error!("dbus::{}] inode:{}] VirtualBlock I/O Operation Failed. {:?}", ctx.method(), &inode, e);
                            return Err(MethodErr::failed("VirtualBlock I/O Operation Failed"));
                        }

                    },
                    _ => {
                        error!("dbus::{}] Inode {} is not a Regular File", ctx.method(), &inode);
                        return Err(MethodErr::invalid_arg("Specified Inode is not a Regular File"));
                    }
                }
            };


            let reply = String::from("Done");

            Ok((reply,))
        });
    });

    let relative_path = shmr_fs.config.mount_dir.canonicalize().unwrap();

    cr.insert(
        relative_path.to_str().unwrap().to_string(),
        &[iface_token],
        shmr_fs,
    );

    info!("starting dbus interface");

    // Serve clients forever.
    cr.serve(&c)?;
    unreachable!()
}
