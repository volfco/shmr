
struct Block {
    /// lightweight hash of the block
    /// used to quickly check if the contents of the block have been changed
    pub hash: Vec<u8>,

    pub topology: (u8, u8),
}
struct INode {

}