use std::time::Instant;

pub struct Bench {
    timer: std::time::Instant,
    tag: String,
}
impl Bench {
    pub fn measure(&mut self, tag: &str, condition: bool) {
        if !condition {
            return;
        }
        let elapsed = self.timer.elapsed();
        if self.tag != "" {
            println!("{}: {:.2?}", self.tag, elapsed);
        }
        self.tag = String::from(tag);
        self.timer = Instant::now();
    }

    pub fn reset(&mut self) {
        self.timer = Instant::now();
        self.tag = String::from("");
    }

    pub fn new() -> Self {
        Bench {
            timer: Instant::now(),
            tag: String::from(""),
        }
    }
}
impl Drop for Bench {
    fn drop(&mut self) {
        let elapsed = self.timer.elapsed();
        if self.tag != "" {
            println!("{}: {:.2?}", self.tag, elapsed);
        }
    }
}
