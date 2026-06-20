import { defineConfig } from 'vite'
import solid from 'vite-plugin-solid'
import UnoCSS from 'unocss/vite'
import { viteSingleFile } from "vite-plugin-singlefile"

export default defineConfig({
  plugins: [solid(), UnoCSS(), viteSingleFile()],
})
