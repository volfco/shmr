
- [ ] Adopt https://github.com/mehcode/config-rs for config file
- [ ] Need to handle open file limits
  - [ ] Allow configuration of the value in config
  - [ ] Worker to monitor open files, and trigger eviction if too high

## Major Features
- Store first few blocks on SSD, while the rest on SMR drives. So the drives can be powered off when not in use