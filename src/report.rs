use crate::args::ReportOptions;
use std::path::Path;

const RESULT_PLACEHOLDER: &str = "<!-- __DATA__ -->";

fn append_result(path: &Path) -> std::io::Result<String> {
    log::debug!("Adding {path:?}");
    let jsonl_data = std::fs::read_to_string(path)?;

    Ok(format!(
        r#"<script type="data" compressed="false">
{jsonl_data}
</script>
"#
    ))
}

pub fn generate_report(args: ReportOptions) -> std::io::Result<()> {
    let report_template_path = std::env::var("RSB_TEMPLATE_PATH")
        .unwrap_or_else(|_| String::from("report/dist/index.html"));

    let mut jsonl_data = String::new();

    for path in args.files {
        jsonl_data.push_str(&append_result(&path)?);
    }

    log::info!("Reading template HTML from {report_template_path}");
    let html = std::fs::read_to_string(report_template_path)?;

    let html = html.replace(RESULT_PLACEHOLDER, &jsonl_data);

    log::info!("Writing finished report to {:?}", args.out);
    std::fs::write(args.out, html)?;

    Ok(())
}
