/* Minimal Cortex-M memory map for the link test (any Cortex-M4F part). */
MEMORY
{
  FLASH : ORIGIN = 0x00000000, LENGTH = 256K
  RAM   : ORIGIN = 0x20000000, LENGTH = 64K
}

ENTRY(Reset);

SECTIONS
{
  .vector_table ORIGIN(FLASH) :
  {
    LONG(ORIGIN(RAM) + LENGTH(RAM));   /* initial stack pointer */
    KEEP(*(.vector_table.reset));      /* reset handler */
  } > FLASH

  .text : { *(.text .text.*); } > FLASH
  .rodata : { *(.rodata .rodata.*); } > FLASH

  .data : ALIGN(64)
  {
    __sdata = .;
    *(.data .data.*);
    . = ALIGN(4);
    __edata = .;
  } > RAM AT > FLASH
  __sidata = LOADADDR(.data);

  .bss (NOLOAD) : ALIGN(64)
  {
    __sbss = .;
    *(.bss .bss.*);
    . = ALIGN(4);
    __ebss = .;
  } > RAM

  /DISCARD/ : { *(.ARM.exidx .ARM.exidx.*); }
}
