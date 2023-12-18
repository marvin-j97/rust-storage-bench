/* @refresh reload */

import "./index.css";
import "virtual:uno.css";

import { render } from "solid-js/web";
import App from "./App";

const root = document.getElementById("root")

render(() => <App />, root!)
