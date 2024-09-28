import "virtual:uno.css";
import "@unocss/reset/tailwind.css";

/* @refresh reload */
import { render } from 'solid-js/web'

import App from './App';

const root = document.getElementById('root')

render(() => <App />, root!)
