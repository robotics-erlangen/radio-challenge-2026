use gpiocdev::{
    Request,
    line::{Bias, EdgeDetection, Value, Values},
};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::Command;

const CONFIG_PATH: &str = "/etc/wpa_supplicant/wpa_supplicant-wlan0.conf";

fn main() -> std::io::Result<()> {
    let pins = [26, 19, 13, 6, 5, 0];
    eprintln!("Started!");

    let chip_path = gpiocdev::chip::chips()
        .unwrap_or_default()
        .into_iter()
        .find(|path| {
            gpiocdev::chip::Chip::from_path(path)
                .and_then(|chip| chip.info())
                .map(|info| info.label == "rp1-gpio" || info.label == "pinctrl-rp1")
                .unwrap_or(false)
        })
        .unwrap_or_else(|| PathBuf::from("/dev/gpiochip4"));

    eprintln!("Using GPIO chip: {:?}", chip_path);

    let request = Request::builder()
        .on_chip(chip_path)
        .with_lines(&pins)
        .as_input()
        .with_bias(Bias::PullUp)
        .as_active_low()
        .with_edge_detection(EdgeDetection::BothEdges)
        .with_consumer("wifi-switcher")
        .request()
        .unwrap();

    eprintln!("Built GPIO request, reading initial states...");

    let mut initial_pin_values = Values::from_offsets(&pins);
    request.values(&mut initial_pin_values).unwrap();
    update_wpa_config(&initial_pin_values, &pins)?;

    for _event in request.edge_events() {
        let mut initial_pin_values = Values::from_offsets(&pins);
        request.values(&mut initial_pin_values).unwrap();
        update_wpa_config(&initial_pin_values, &pins)?;
    }

    Ok(())
}

fn update_wpa_config(pin_values: &Values, pins: &[u32]) -> std::io::Result<()> {
    let mut jumper_state = Vec::new();
    for &pin in pins {
        if pin_values.get(pin) == Some(Value::Active) {
            jumper_state.push(true);
        } else {
            jumper_state.push(false);
        }
    }
    eprintln!("New jumper state: {:?}", jumper_state);
    // Select just the first network if no jumpers are connected
    if !jumper_state.iter().any(|a| *a) {
        jumper_state[0] = true;
    }

    let conf_state = read_enabled_states()?;
    eprintln!("Current wpa_supplicant state: {:?}", conf_state);
    if jumper_state != conf_state {
        set_enabled_states(&jumper_state)?;
    }

    Ok(())
}

/// Parses wpa_supplicant.conf and returns if each network block is enabled (true) or disabled using "disabled=1" (false).
fn read_enabled_states() -> std::io::Result<Vec<bool>> {
    let mut states = Vec::new();
    let file = File::open(CONFIG_PATH)?;

    let lines = BufReader::new(file).lines().map_while(Result::ok);
    let mut inside_network = false;
    let mut is_disabled = false;

    for line in lines {
        let trimmed = line.trim();
        let collapsed = trimmed.replace(" ", "");

        if collapsed.starts_with("network={") {
            inside_network = true;
            is_disabled = false; // Default value is enabled
        } else if inside_network {
            if collapsed.starts_with("disabled=1") {
                is_disabled = true;
            } else if trimmed == "}" {
                states.push(!is_disabled);
                inside_network = false;
            }
        }
    }

    Ok(states)
}

/// Sets the `disabled` flags for all networks in CONFIG_PATH.
fn set_enabled_states(states: &[bool]) -> std::io::Result<()> {
    let file = File::open(CONFIG_PATH)?;
    let lines = BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .collect::<Vec<String>>();

    let mut new_lines = Vec::new();
    let mut network_idx = -1;
    let mut inside_network = false;
    let mut found_disabled_field = false;

    for line in lines {
        let trimmed = line.trim();
        let collapsed = trimmed.replace(" ", "");

        // Network block start
        if collapsed.starts_with("network={") {
            inside_network = true;
            network_idx += 1;
            found_disabled_field = false;
            new_lines.push(line);
            continue;
        }

        if inside_network {
            // Existing "disabled" option, replace or remove
            if collapsed.starts_with("disabled=") {
                found_disabled_field = true;
                let idx = network_idx as usize;

                if idx < states.len() {
                    // Has data for this network, set or remove
                    if states[idx] {
                        // Don't re-emit == remove "disable" == re-enable
                    } else {
                        // Replace, just in case the old one was set to "disabled=0"
                        new_lines.push("    disabled=1".to_string());
                    }
                } else {
                    // Pass through the existing state of any additional networks (more than provided states)
                    new_lines.push(line);
                }
                continue;
            }

            // Network block end
            if trimmed == "}" {
                inside_network = false;
                let idx = network_idx as usize;

                // Should be disabled, but no existing line found -> append
                if idx < states.len() && !states[idx] && !found_disabled_field {
                    new_lines.push("    disabled=1".to_string());
                }
                new_lines.push(line);
                continue;
            }
        }

        new_lines.push(line);
    }

    // Save changes
    if let Ok(mut file) = File::create(CONFIG_PATH) {
        for line in new_lines {
            let _ = writeln!(file, "{}", line);
        }
    }

    // Attempt to reload using wpa_cli
    let reload_status = Command::new("wpa_cli")
        .args(["-i", "wlan0", "reconfigure"])
        .status();

    if !reload_status.is_ok_and(|s| s.success()) {
        println!("Failed to reload wpa_supplicant.conf");
    }

    Ok(())
}
