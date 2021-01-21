# Fun Rust MapReduce Word Count with Tokio.rs

Simple MapReduce Word Count using Tokio.rs for fun.

    rust-map-reduce 1.0
    Kevin M. <kevin.margueritte@gmail.com>
    MapReduce Word Count with Tokio.rs
    
    USAGE:
        rust-map-reduce [OPTIONS]
    
    FLAGS:
        -h, --help       Prints help information
        -V, --version    Prints version information
    
    OPTIONS:
        -i, --input <input>
                Sets file to read [default: samples/The_Extraordinary_Adventures_of_Arsene_Lupin.txt]
    
        -m, --mappers-number <mappers-number>      Sets number of mappers [default: 5]
        -o, --output <output>
                Sets file to write, by default the result is printed on stdout
    
        -r, --reducers-number <reducers-number>    Sets number of reducers [default: 2]
