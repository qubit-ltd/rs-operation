use std::error::Error;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use qubit_operation::{
    ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode,
    ShutdownWait,
};

struct InputFile {
    file: Mutex<File>,
}

impl InputFile {
    fn open(path: PathBuf) -> io::Result<Self> {
        Ok(Self {
            file: Mutex::new(File::open(path)?),
        })
    }

    fn detect(&self) -> io::Result<Option<&'static str>> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("input lock poisoned"))?;
        let mut header = [0_u8; 5];
        file.read_exact(&mut header)?;
        Ok((header == *b"%PDF-").then_some("application/pdf"))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: detect_file <input-path>")?;
    let manager = OperationManager::new(ManagerOptions::default())?;

    let mut operation = manager.try_submit(OperationOptions::default(), move |scope| {
        let file = InputFile::open(path).map_err(|e| e.to_string())?;
        let input = scope
            .manage(file)
            .map_err(|error| format!("{:?}", error.kind()))?;
        input.detect().map_err(|e| e.to_string())
    })?;

    let outcome = operation.wait_outcome();
    let settlement = operation.wait_settlement();
    let closing = manager.request_shutdown(ShutdownMode::Drain);
    let shutdown = closing.wait_for(Duration::from_secs(1));

    // 三次观察均已进行，再处理各自的返回值。
    match outcome? {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        OperationOutcome::Succeeded(None) => println!("MIME: no match"),
        other => eprintln!("detection failed: {other:?}"),
    }

    let report = settlement?;
    if report.all_released() {
        println!("input cleanup completed");
    } else {
        eprintln!("cleanup requires attention: {:?}", report.status);
    }
    for failure in report.cleanup_failures.iter() {
        eprintln!("cleanup failure: {failure:?}");
    }

    match shutdown? {
        ShutdownWait::Completed(_) => println!("manager closed"),
        ShutdownWait::Pending(snapshot) => {
            eprintln!("shutdown pending: {snapshot:?}");
            // 本例继续等待；服务中可保存 closing，稍后再次观察。
            let _report = closing.wait()?;
            println!("manager closed");
        }
    }
    Ok(())
}
